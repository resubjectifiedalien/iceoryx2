// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::format;

use iceoryx2::node::Node;
use iceoryx2::port::LoanError;
use iceoryx2::service::{static_config::StaticConfig, Service};
use iceoryx2_log::{fail, trace, warn};
use iceoryx2_tunnel_backend::{
    traits::RequestResponseRelay,
    types::request_response::{Client, Header, Payload, Server},
};

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum CreationError {
    Service,
    Client,
    Server,
}

impl core::fmt::Display for CreationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "CreationError::{self:?}")
    }
}

impl core::error::Error for CreationError {}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Error {
    RequestDelivery,
    RequestIngestion,
    ResponseDelivery,
    ResponseIngestion,
    LocalRequestReceive,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Error::{self:?}")
    }
}

impl core::error::Error for Error {}

#[derive(Debug)]
pub(crate) struct RequestResponsePorts<S: Service> {
    pub(crate) static_config: StaticConfig,
    pub(crate) client: Client<S>,
    pub(crate) server: Server<S>,
}

impl<S: Service> RequestResponsePorts<S> {
    pub(crate) fn new(static_config: &StaticConfig, node: &Node<S>) -> Result<Self, CreationError> {
        let origin = format!(
            "RequestResponsePorts<{}>::new",
            core::any::type_name::<S>()
        );

        let port_config = static_config.request_response();
        let service = unsafe {
            fail!(
                from origin,
                when node.service_builder(static_config.name())
                        .request_response::<Payload, Payload>()
                        .request_user_header::<Header>()
                        .response_user_header::<Header>()
                        .__internal_set_request_header_type_details(
                             &port_config.request_message_type_details().user_header
                        )
                        .__internal_set_request_payload_type_details(
                             &port_config.request_message_type_details().payload
                        )
                         .__internal_set_response_header_type_details(
                             &port_config.response_message_type_details().user_header
                        )
                        .__internal_set_response_payload_type_details(
                             &port_config.response_message_type_details().payload
                        )
                        .enable_safe_overflow_for_requests(port_config.has_safe_overflow_for_requests())
                        .enable_safe_overflow_for_responses(port_config.has_safe_overflow_for_responses())
                        .max_nodes(port_config.max_nodes())
                        .max_servers(port_config.max_servers())
                        .max_clients(port_config.max_clients())
                        .max_active_requests_per_client(port_config.max_active_requests_per_client())
                        .max_response_buffer_size(port_config.max_response_buffer_size())
                        .max_loaned_requests(port_config.max_loaned_requests())
                        .max_borrowed_responses_per_pending_response(port_config.max_borrowed_responses_per_pending_response())
                        .enable_fire_and_forget_requests(port_config.does_support_fire_and_forget_requests())
                        .open_or_create(),
                with CreationError::Service,
                "Failed to open or create service {}({})", static_config.messaging_pattern(), static_config.name()
            )
        };

        let client = fail!(
            from origin,
            when service.client_builder().create(),
            with CreationError::Client,
            "Failed to create Client for {}({})", static_config.messaging_pattern(), static_config.name()
        );

        let server = fail!(
            from origin,
            when service.server_builder().create(),
            with CreationError::Server,
             "Failed to create Server for {}({})", static_config.messaging_pattern(), static_config.name()
        );

        Ok(RequestResponsePorts {
            static_config: static_config.clone(),
            client,
            server,
        })
    }

    pub(crate) fn maintain<E1, E2, E3, E4>(
        &self,
        relay: &impl RequestResponseRelay<
            S,
            SendRequestError = E1,
            ReceiveRequestError = E2,
            SendResponseError = E3,
            ReceiveResponseError = E4,
        >,
    ) -> Result<(), Error>
    where
        E1: core::error::Error + 'static,
        E2: core::error::Error + 'static,
        E3: core::error::Error + 'static,
        E4: core::error::Error + 'static,
    {
        // 1. Receive Local Requests and Forward via Relay
        loop {
            match self.server.receive() {
                Ok(Some(request)) => {
                    trace!(from self, "Forwarding Request for {}({})", self.static_config.messaging_pattern(), self.static_config.name());
                    fail!(
                         from self,
                         when relay.send_request(request),
                         with Error::RequestDelivery,
                         "Failed to deliver request to relay"
                    );
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(from self, "Failed to receive local request: {:?}", e);
                    return Err(Error::LocalRequestReceive);
                }
            }
        }

        // 2. Receive Remote Requests from Relay and Forward to Local Service (Client Proxy)
        loop {
            let result = relay.receive_request(&mut |len| unsafe { self.client.loan_custom_payload(len) });
            match result {
                Ok(Some((request_mut, context))) => {
                    trace!(from self, "Received Remote Request for {}({})", self.static_config.messaging_pattern(), self.static_config.name());
                    match request_mut.send() {
                        Ok(pending_response) => {
                            fail!(
                                from self,
                                when relay.submit_pending_response(pending_response, context),
                                with Error::ResponseDelivery,
                                "Failed to submit pending response to relay"
                            );
                        }
                        Err(e) => {
                            warn!(from self, "Failed to send request to local service: {:?}", e);
                            return Err(Error::RequestIngestion);
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(from self, "Failed to receive remote request from relay: {:?}", e);
                    return Err(Error::RequestIngestion);
                }
            }
        }
        
        // 3. Receive Remote Responses from Relay and Forward to Local ActiveRequest (Server Proxy)
        loop {
            // We pass a dummy loan function because the relay implementation should use
            // ActiveRequest::loan_uninit via its stored state.
            let result = relay.receive_response(&mut |_| Err(LoanError::InternalFailure));
            match result {
                Ok(Some(response_mut)) => {
                    trace!(from self, "Forwarding Remote Response for {}({})", self.static_config.messaging_pattern(), self.static_config.name());
                    fail!(
                        from self,
                        when response_mut.send(),
                        with Error::ResponseDelivery, 
                        "Failed to send response to local client"
                    );
                }
                 Ok(None) => break,
                 Err(e) => {
                     warn!(from self, "Failed to receive remote response from relay: {:?}", e);
                     return Err(Error::ResponseIngestion);
                 }
            }
        }

        Ok(())
    }
}
