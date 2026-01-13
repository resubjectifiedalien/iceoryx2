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

use core::mem::MaybeUninit;

use iceoryx2::service::builder::CustomHeaderMarker;
use iceoryx2::service::builder::CustomPayloadMarker;

pub type Header = CustomHeaderMarker;
pub type Payload = [CustomPayloadMarker];
pub type PayloadUninit = [MaybeUninit<CustomPayloadMarker>];

// 5 generics
pub type Request<S> = iceoryx2::active_request::ActiveRequest<S, Payload, Header, Payload, Header>;
pub type RequestMut<S> = iceoryx2::request_mut::RequestMut<S, Payload, Header, Payload, Header>;
pub type RequestMutUninit<S> = iceoryx2::request_mut_uninit::RequestMutUninit<
    S,
    PayloadUninit,
    Header,
    Payload,
    Header,
>;

// 3 generics
pub type Response<S> = iceoryx2::response::Response<S, Payload, Header>;
pub type ResponseMut<S> = iceoryx2::response_mut::ResponseMut<S, Payload, Header>;
pub type ResponseMutUninit<S> =
    iceoryx2::response_mut_uninit::ResponseMutUninit<S, Payload, Header>;

// Client: 5 generics (sending Request, receiving Response)
pub type Client<S> = iceoryx2::port::client::Client<S, Payload, Header, Payload, Header>;
pub type PendingResponse<S> = match_event::PendingResponse<S, Payload, Header, Payload, Header>;
// Server: 5 generics (receiving Request, sending Response)
pub type Server<S> = iceoryx2::port::server::Server<S, Payload, Header, Payload, Header>;

use iceoryx2::pending_response as match_event;

pub type LoanRequestFn<'a, S, LoanError> =
    dyn FnMut(usize) -> Result<RequestMutUninit<S>, LoanError> + 'a;

pub type LoanResponseFn<'a, S, LoanError> =
    dyn FnMut(usize) -> Result<ResponseMutUninit<S>, LoanError> + 'a;
