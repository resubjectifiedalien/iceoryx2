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

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use iceoryx2::service::{
    builder::CustomHeaderMarker,
    header::request_response::{RequestHeader, ResponseHeader},
    static_config::StaticConfig,
    Service,
};
use iceoryx2_log::{fail, warn};
use iceoryx2_tunnel_backend::{
    traits::{RequestResponseRelay, RelayBuilder},
    types::request_response::{
        LoanRequestFn, LoanResponseFn, PendingResponse, Request, RequestMut, ResponseMut,
    },
};
use zenoh::{
    bytes::ZBytes,
    handlers::{FifoChannel, FifoChannelHandler},
    pubsub::{Publisher, Subscriber},
    qos::Reliability,
    sample::{Locality, Sample},
    Session, Wait,
};

use crate::keys;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum CreationError {
    RequestPublisherDeclaration,
    RequestSubscriberDeclaration,
    ResponsePublisherDeclaration,
    ResponseSubscriberDeclaration,
}

impl core::fmt::Display for CreationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "CreationError::{self:?}")
    }
}

impl core::error::Error for CreationError {}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SendError {
    PayloadPut,
    Serialization,
}

impl core::fmt::Display for SendError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "SendError::{self:?}")
    }
}

impl core::error::Error for SendError {}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum ReceiveError {
    SampleReceive,
    IceoryxLoan,
    Deserialization,
}

impl core::fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ReceiveError::{self:?}")
    }
}

impl core::error::Error for ReceiveError {}

#[derive(Debug)]
pub struct Builder<'a, S: Service> {
    session: &'a Session,
    static_config: &'a StaticConfig,
    _phantom: core::marker::PhantomData<S>,
}

impl<'a, S: Service> Builder<'a, S> {
    pub fn new(session: &'a Session, static_config: &'a StaticConfig) -> Builder<'a, S> {
        Builder {
            session,
            static_config,
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<'a, S: Service> RelayBuilder for Builder<'a, S> {
    type CreationError = CreationError;
    type Relay = Relay<S>;

    fn create(self) -> Result<Self::Relay, Self::CreationError> {
        let req_key = keys::request(self.static_config.service_id());
        let res_key = keys::response(self.static_config.service_id());

        let request_publisher = fail!(
            from self,
            when self.session
                .declare_publisher(req_key.clone())
                .allowed_destination(Locality::Remote)
                .reliability(Reliability::Reliable)
                .wait(),
            with CreationError::RequestPublisherDeclaration,
            "Failed to create zenoh publisher for requests"
        );

        let request_subscriber = fail!(
            from self,
            when self.session
                .declare_subscriber(req_key.clone())
                .with(FifoChannel::new(10))
                .allowed_origin(Locality::Remote)
                .wait(),
            with CreationError::RequestSubscriberDeclaration,
            "Failed to create zenoh subscriber for requests"
        );

        let response_publisher = fail!(
            from self,
            when self.session
                .declare_publisher(res_key.clone())
                .allowed_destination(Locality::Remote)
                .reliability(Reliability::Reliable)
                .wait(),
            with CreationError::ResponsePublisherDeclaration,
            "Failed to create zenoh publisher for responses"
        );

        let response_subscriber = fail!(
            from self,
            when self.session
                .declare_subscriber(res_key.clone())
                .with(FifoChannel::new(10))
                .allowed_origin(Locality::Remote)
                .wait(),
            with CreationError::ResponseSubscriberDeclaration,
            "Failed to create zenoh subscriber for responses"
        );

        Ok(Relay {
            static_config: self.static_config.clone(),
            request_publisher,
            request_subscriber,
            response_publisher,
            response_subscriber,
            pending_local_requests: Mutex::new(HashMap::new()),
            pending_remote_requests: Mutex::new(Vec::new()),
        })
    }
}

#[derive(Debug)]
pub struct Relay<S: Service> {
    static_config: StaticConfig,
    request_publisher: Publisher<'static>,
    request_subscriber: Subscriber<FifoChannelHandler<Sample>>,
    response_publisher: Publisher<'static>,
    response_subscriber: Subscriber<FifoChannelHandler<Sample>>,
    
    // Key: (UniqueClientId, RequestId) -> Request Object
    pending_local_requests: Mutex<HashMap<(u128, u64), Request<S>>>,
    
    // List of pending responses from local servers that need to be sent to remote
    // (PendingResponse, OriginalRequestHeader)
    pending_remote_requests: Mutex<Vec<(PendingResponse<S>, RequestHeader)>>,
}

impl<S: Service> Relay<S> {
    fn request_response_config(&self) -> &iceoryx2::service::static_config::request_response::StaticConfig {
        self.static_config.request_response()
    }
}

impl<S: Service> RequestResponseRelay<S> for Relay<S> {
    type RequestContext = RequestHeader;
    type SendRequestError = SendError;
    type ReceiveRequestError = ReceiveError;
    type SendResponseError = SendError;
    type ReceiveResponseError = ReceiveError;

    fn send_request(&self, request: Request<S>) -> Result<(), Self::SendRequestError> {
        let header = request.header();
        let key = (header.client_id().value(), header.request_id());
        
        let header_bytes = unsafe {
             core::slice::from_raw_parts(
                 (header as *const RequestHeader).cast::<u8>(),
                 core::mem::size_of::<RequestHeader>()
             )
        };
        let user_header_size = self.request_response_config().request_message_type_details().user_header.size() as usize;
        let user_header_bytes = unsafe {
             core::slice::from_raw_parts(
                 (request.user_header() as *const CustomHeaderMarker).cast::<u8>(),
                 user_header_size
             )
        };
        let payload = request.payload();
        let element_size = self.request_response_config().request_message_type_details().payload.size() as usize;
        let payload_len_bytes = if payload.len() == 1 { payload.len() * element_size } else { payload.len() };

        let payload_bytes = unsafe {
             core::slice::from_raw_parts(
                 payload.as_ptr().cast::<u8>(),
                 payload_len_bytes 
             )
        };
        
        let mut msg = Vec::with_capacity(header_bytes.len() + user_header_bytes.len() + payload_bytes.len());
        msg.extend_from_slice(header_bytes);
        msg.extend_from_slice(user_header_bytes);
        msg.extend_from_slice(payload_bytes);
        
        // Move request to map
        self.pending_local_requests.lock().unwrap().insert(key, request);

        let z_payload = ZBytes::from(msg);

        fail!(
            from self,
            when self.request_publisher.put(z_payload).wait(),
            with SendError::PayloadPut,
            "Failed to publish request"
        );

        Ok(())
    }

    fn receive_request<LoanError>(
        &self,
        loan: &mut LoanRequestFn<'_, S, LoanError>,
    ) -> Result<Option<(RequestMut<S>, Self::RequestContext)>, Self::ReceiveRequestError> {
        let zenoh_sample = fail!(
            from self,
            when self.request_subscriber.try_recv(),
            with ReceiveError::SampleReceive,
            "Failed to receive request sample"
        );

        if let Some(zenoh_sample) = zenoh_sample {
            let payload_bytes = zenoh_sample.payload().to_bytes();
            let total_len = payload_bytes.len();
            
            let header_size = core::mem::size_of::<RequestHeader>();
            let user_header_size = self.request_response_config().request_message_type_details().user_header.size() as usize;
            
            if total_len < header_size + user_header_size {
                 warn!(from self, "Received request too small");
                 return Ok(None);
            }

            let header_ptr = payload_bytes.as_ptr();
            let header = unsafe { core::ptr::read_unaligned(header_ptr as *const RequestHeader) };
            
            let user_header_ptr = unsafe { header_ptr.add(header_size) };
            let data_ptr = unsafe { user_header_ptr.add(user_header_size) };
            let data_len_bytes = total_len - header_size - user_header_size;
            
            let element_size = self.request_response_config().request_message_type_details().payload.size() as usize;
            if element_size == 0 {
                warn!(from self, "Element size is 0, cannot determine number of elements");
                return Ok(None);
            }
            if data_len_bytes % element_size != 0 {
                warn!(from self, "Data length {} is not multiple of element size {}", data_len_bytes, element_size);
                return Ok(None);
            }
            let number_of_elements = data_len_bytes / element_size;

            let mut request_mut = fail!(
                from self,
                when loan(number_of_elements),
                with ReceiveError::IceoryxLoan,
                "Failed to loan request"
            );
            
            unsafe {
                core::ptr::copy_nonoverlapping(
                    data_ptr,
                    request_mut.payload_mut().as_mut_ptr() as *mut u8,
                    data_len_bytes
                );
            }
            // Write User Header
            unsafe {
                 core::ptr::copy_nonoverlapping(
                     user_header_ptr,
                     (request_mut.user_header_mut() as *mut CustomHeaderMarker) as *mut u8,
                     user_header_size
                 );
            }
            
            let request_mut = unsafe { request_mut.assume_init() }; 
            
            return Ok(Some((request_mut, header)));
        }

        Ok(None)
    }

    fn submit_pending_response(
        &self,
        response: PendingResponse<S>,
        context: Self::RequestContext,
    ) -> Result<(), Self::SendResponseError> {
        self.pending_remote_requests.lock().unwrap().push((response, context));
        Ok(())
    }

    fn receive_response<LoanError>(
        &self,
        loan: &mut LoanResponseFn<'_, S, LoanError>,
    ) -> Result<Option<ResponseMut<S>>, Self::ReceiveResponseError> {
        // 1. Process pending remote requests
        let mut completed_indices = Vec::new(); 
        let mut pending_lock = self.pending_remote_requests.lock().unwrap();
        
        for (i, (pending, original_header)) in pending_lock.iter().enumerate() {
             if !pending.is_connected() {
                 completed_indices.push(i);
                 continue;
             }
             
             match unsafe { pending.receive_custom_payload() } {
                 Ok(Some(response)) => {
                      // Serialize Response
                      let response_header = response.header();
                      let res_header_bytes = unsafe {
                          core::slice::from_raw_parts(
                               (response_header as *const ResponseHeader).cast::<u8>(),
                               core::mem::size_of::<ResponseHeader>()
                          )
                      };
                      let orig_header_bytes = unsafe {
                           core::slice::from_raw_parts(
                               (original_header as *const RequestHeader).cast::<u8>(),
                               core::mem::size_of::<RequestHeader>()
                           )
                      };
                      
                      let user_header_size = self.request_response_config().response_message_type_details().user_header.size() as usize;
                      let user_header_bytes = unsafe {
                           core::slice::from_raw_parts(
                               (response.user_header() as *const CustomHeaderMarker).cast::<u8>(),
                               user_header_size
                           )
                      };
                      
                      let payload = response.payload();
                      let element_size = self.request_response_config().response_message_type_details().payload.size() as usize;
                       let payload_len_bytes = if payload.len() == 1 { payload.len() * element_size } else { payload.len() };
                      let payload_bytes = unsafe {
                           core::slice::from_raw_parts(
                               payload.as_ptr().cast::<u8>(),
                               payload_len_bytes
                           )
                      };
                      
                      let mut msg = Vec::with_capacity(orig_header_bytes.len() + res_header_bytes.len() + user_header_bytes.len() + payload_bytes.len());
                      msg.extend_from_slice(orig_header_bytes);
                      msg.extend_from_slice(res_header_bytes);
                      msg.extend_from_slice(user_header_bytes);
                      msg.extend_from_slice(payload_bytes);
                      
                      let z_payload = ZBytes::from(msg);
                      let _ = self.response_publisher.put(z_payload).wait(); 
                      
                 },
                 Ok(None) => {}, 
                 Err(_) => {
                      completed_indices.push(i); 
                 }
             }
        }
        
        for i in completed_indices.iter().rev() {
             pending_lock.remove(*i);
        }
        drop(pending_lock);

        // 2. Receive remote responses
        let zenoh_sample = fail!(
            from self,
            when self.response_subscriber.try_recv(),
            with ReceiveError::SampleReceive,
            "Failed to receive response sample"
        );
        
        if let Some(zenoh_sample) = zenoh_sample {
            let payload_bytes = zenoh_sample.payload().to_bytes();
            let total_len = payload_bytes.len();
            
            let req_header_size = core::mem::size_of::<RequestHeader>();
            let res_header_size = core::mem::size_of::<ResponseHeader>();
            let user_header_size = self.request_response_config().response_message_type_details().user_header.size() as usize;
            
            if total_len < req_header_size + res_header_size + user_header_size {
                 return Ok(None);
            }
            
            let req_header_ptr = payload_bytes.as_ptr();
            let req_header = unsafe { core::ptr::read_unaligned(req_header_ptr as *const RequestHeader) };
            
            let res_header_ptr = unsafe { req_header_ptr.add(req_header_size) };
            let user_header_ptr = unsafe { res_header_ptr.add(res_header_size) };
            let data_ptr = unsafe { user_header_ptr.add(user_header_size) };
            let data_len_bytes = total_len - req_header_size - res_header_size - user_header_size;
            
            // Find request
            let key = (req_header.client_id().value(), req_header.request_id());
            let mut map = self.pending_local_requests.lock().unwrap();
            
            if let Some(request) = map.remove(&key) {
                 let element_size = self.request_response_config().response_message_type_details().payload.size() as usize;
                 if element_size == 0 {
                      return Ok(None);
                 }
                 let number_of_elements = data_len_bytes / element_size;

                 let mut response_uninit = fail!(
                      from self,
                      when unsafe { request.loan_custom_payload(number_of_elements) },
                      with ReceiveError::IceoryxLoan,
                      "Failed to loan response"
                 );
                 
                 // Write payload
                 unsafe {
                      core::ptr::copy_nonoverlapping(
                          data_ptr,
                          response_uninit.payload_mut().as_mut_ptr() as *mut u8,
                          data_len_bytes
                      );
                 }
                 unsafe {
                       core::ptr::copy_nonoverlapping(
                           user_header_ptr,
                           (response_uninit.user_header_mut() as *mut CustomHeaderMarker) as *mut u8,
                           user_header_size
                       );
                 }
                 
                 // Finalize
                 let response_mut = unsafe { response_uninit.assume_init() };
                 
                 return Ok(Some(response_mut));
            } else {
                 return Ok(None);
            }
        }
        
        Ok(None)
    }
}
