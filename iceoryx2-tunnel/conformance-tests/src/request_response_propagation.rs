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

use iceoryx2_bb_conformance_test_macros::conformance_test_module;

#[allow(clippy::module_inception)]
#[conformance_test_module]
pub mod request_response_propagation {
    use core::fmt::Debug;
    use core::time::Duration;

    use iceoryx2::prelude::*;
    use iceoryx2::testing::*;

    use iceoryx2::service::Service;
    use iceoryx2_bb_conformance_test_macros::conformance_test;
    use iceoryx2_bb_posix::unique_system_id::UniqueSystemId;
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_bb_testing::test_fail;
    use iceoryx2_tunnel::Tunnel;
    use iceoryx2_tunnel_backend::traits::{testing::Testing, Backend};

    fn generate_service_name() -> ServiceName {
        ServiceName::new(&format!(
            "request_response_relay_tests_{}",
            UniqueSystemId::new().unwrap().value()
        ))
        .unwrap()
    }

    fn propagate_simple_request<S: Service, B: Backend<S> + Debug, T: Testing>() {
        const MAX_ATTEMPTS: usize = 25;
        const TIMEOUT: Duration = Duration::from_millis(50); // Fast timeout for loop polling

        // === SETUP ===
        let service_name = generate_service_name();

        // --- Host A (Client) ---
        let iceoryx_config_a = generate_isolated_config();
        let backend_config_a = B::Config::default();
        let tunnel_config_a = iceoryx2_tunnel::Config::default();
        let mut tunnel_a =
            Tunnel::<S, B>::create(&tunnel_config_a, &iceoryx_config_a, &backend_config_a).unwrap();

        let node_a = NodeBuilder::new()
            .config(&iceoryx_config_a)
            .create::<S>()
            .unwrap();
        
        let service_a = node_a
            .service_builder(&service_name)
            .request_response::<u64, u64>()
            .open_or_create()
            .unwrap();
        let client_a = service_a.client_builder().create().unwrap();

        tunnel_a.discover_over_iceoryx().unwrap();

        // --- Host B (Server) ---
        let iceoryx_config_b = generate_isolated_config();
        let backend_config_b = B::Config::default();
        let tunnel_config_b = iceoryx2_tunnel::Config::default();
        let mut tunnel_b =
            Tunnel::<S, B>::create(&tunnel_config_b, &iceoryx_config_b, &backend_config_b).unwrap();
        
        // Wait for tunnel B discovery of service A
        T::retry(
            || {
                tunnel_b.discover_over_backend().unwrap();
                let services = tunnel_b.tunneled_services();
                if services.len() == 1 && services.contains(service_a.service_id()) {
                    return Ok(());
                }
                 // Also propagate tunnel a so it advertises
                tunnel_a.propagate().unwrap();
                
                Err("Failed to discover remote services")
            },
            Duration::from_secs(1),
            Some(20),
        ).unwrap();

        // Create Server on B (Service B created by Tunnel B)
        // Note: Tunnel B might have created the service.
        let node_b = NodeBuilder::new()
            .config(&iceoryx_config_b)
            .create::<S>()
            .unwrap();
        let service_b = node_b
            .service_builder(&service_name)
            .request_response::<u64, u64>()
            .open_or_create()
            .unwrap();
        
        let server_b = service_b.server_builder().create().unwrap();

        // Connect Client A (Send request)
        let request_payload: u64 = 123456;
        let pending = client_a.send_copy(request_payload).unwrap();

        // Loop to propagate
        let mut response_received = false;
        
        for _ in 0..100 {
            tunnel_a.propagate().unwrap();
            tunnel_b.propagate().unwrap();
            
            // Server B receive?
            if let Ok(Some(request)) = server_b.receive() {
                assert_that!(*request, eq request_payload);
                request.send_copy(request_payload * 2).unwrap();
            }
            
            // Client A receive?
            if let Ok(Some(response)) = pending.receive() {
                assert_that!(*response, eq request_payload * 2);
                response_received = true;
                break;
            }
            
            std::thread::sleep(TIMEOUT);
        }
        
        assert_that!(response_received, eq true);
    }
    
    #[conformance_test]
    pub fn propagate_simple_request_test<S: Service, B: Backend<S> + Debug, T: Testing>() {
        propagate_simple_request::<S, B, T>()
    }
}
