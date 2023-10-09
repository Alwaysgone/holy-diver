use std::{num::NonZeroU8, path::PathBuf, net::SocketAddr, str::FromStr, sync::{Arc, Mutex}};

use foca::Config;
use swim::{foca::setup_foca, types::ID, core::{FocaRuntimeConfig, HolyDiverDataHandler, HolyDiverController}};

use wasm_bindgen::prelude::*;

pub mod swim;

#[wasm_bindgen]
pub struct HolyDiverHolder {
    controller: Arc<Mutex<HolyDiverController>>,
}

#[wasm_bindgen]
pub async fn init(data_dir_path: &str, bind_address: &str) -> HolyDiverHolder {
    let foca_config = {
        let mut c = Config::simple();
        // With this setting you can suspend (^Z) one process,
        // wait for it the member to be declared down then resume
        // it (fg) and foca should recover by itself
        c.notify_down_members = true;
        // limits the number of broadcasts of a single message
        c.max_transmissions = NonZeroU8::new(2).unwrap();
        c
    };
    let mut data_dir = PathBuf::new();
    data_dir.push(data_dir_path);
    let bind_addr = SocketAddr::from_str(bind_address).unwrap();
    let identity = ID::new(bind_addr);
    let announce_to = None;
    let runtime_config = FocaRuntimeConfig {
        identity: identity.clone(),
        data_dir,
        bind_addr,
        announce_to,
        foca_config,
    };
    let data_handler = Arc::from(Mutex::from(HolyDiverDataHandler::new(&runtime_config.data_dir, identity.clone())));
    let foca_command_sender = setup_foca(runtime_config, Box::new(data_handler.clone())).await.unwrap();
    let controller = Arc::from(Mutex::from(HolyDiverController {
        data_handler,
        foca_command_sender,
    }));
    HolyDiverHolder {
        controller
    }
}

#[wasm_bindgen]
pub async fn set_field(holder: HolyDiverHolder, field_name: String, field_value: String) {
    holder.controller.lock().unwrap().set_field(field_name, field_value).await.unwrap();
}

#[wasm_bindgen]
pub fn get_field(holder: HolyDiverHolder, field_name: String) -> String {
    holder.controller.lock().unwrap().get_field(field_name).unwrap_or("N/A".to_owned())
}
