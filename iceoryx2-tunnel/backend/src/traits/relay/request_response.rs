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

use core::error::Error;

use iceoryx2::service::Service;

use crate::types::request_response::{
    LoanRequestFn, LoanResponseFn, PendingResponse, Request, RequestMut, ResponseMut,
};

/// Relay for tunneling iceoryx2 request-response samples through a backend.
/// 
/// [`RequestResponseRelay`] enables bi-directional transmission of [`Request`]s and [`PendingResponse`]s
/// between local iceoryx2 [`Service`]s and remote [`Service`]s via the
/// [`Backend`](crate::traits::Backend) communication mechanism.
/// 
/// # Type Parameters
/// 
/// * `S` - The iceoryx2 [`Service`] type
/// 
/// # Memory Management
/// 
/// Received [`Request`]s are ingested into iceoryx2 shared memory using a loan
/// function, which allocates memory from the local shared memory pool. This
/// enables efficient zero-copy delivery to local participants.
/// 
/// # Examples
/// 
/// Sending a [`Request`] over the [`Backend`](crate::traits::Backend):
/// 
/// ```no_run
/// # use iceoryx2_tunnel_backend::traits::RequestResponseRelay;
/// # use iceoryx2::service::ipc::Service;
/// # fn example<R: RequestResponseRelay<Service>>(relay: &R, request: Request<Service>) -> Result<(), R::SendRequestError> {
/// relay.send_request(request)?;
/// # Ok(())
/// # }
/// ```
/// 
pub trait RequestResponseRelay<S: Service> {
    /// Context type for pairing requests and responses.
    type RequestContext: core::fmt::Debug + Send;

    /// Error type returned when sending a request fails.
    type SendRequestError: Error + 'static;

    /// Error type returned when receiving a request fails.
    type ReceiveRequestError: Error + 'static;

    /// Error type returned when sending a response fails.
    type SendResponseError: Error + 'static;

    /// Error type returned when receiving a response fails.
    type ReceiveResponseError: Error + 'static;

    /// Sends a [`Request`] via the backend communication mechanism.
    ///
    /// The [`Request`] (which is an alias for [`ActiveRequest`](iceoryx2::active_request::ActiveRequest))
    /// payload and header are transmitted to the remote endpoint.
    fn send_request(&self, request: Request<S>) -> Result<(), Self::SendRequestError>;

    /// Attempts to receive a request via the backend communication mechanism.
    ///
    /// Checks for incoming requests without blocking. If a request is available,
    /// it allocates shared memory via the provided loan function (from the local client port)
    /// and deserializes sending the [`RequestMut`] data into that memory.
    ///
    /// # Returns
    ///
    /// * [`RequestMut`] - A request was successfully received and initialized.
    /// * [`None`] when no request is available.
    fn receive_request<LoanError>(
        &self,
        loan: &mut LoanRequestFn<'_, S, LoanError>,
    ) -> Result<Option<(RequestMut<S>, Self::RequestContext)>, Self::ReceiveRequestError>;

    /// Submits a [`PendingResponse`] to the backend.
    ///
    /// The backend takes ownership of the pending response and is responsible for
    /// polling it and transmitting the response when available, using the provided context.
    fn submit_pending_response(
        &self,
        response: PendingResponse<S>,
        context: Self::RequestContext,
    ) -> Result<(), Self::SendResponseError>;

    /// Attempts to receive a response via the backend communication mechanism.
    ///
    /// Checks for incoming responses without blocking.
    ///
    /// # Returns
    ///
    /// * [`ResponseMut`] - A response was successfully received and initialized.
    /// * [`None`] when no response is available.
    fn receive_response<LoanError>(
        &self,
        loan: &mut LoanResponseFn<'_, S, LoanError>,
    ) -> Result<Option<ResponseMut<S>>, Self::ReceiveResponseError>;
}
