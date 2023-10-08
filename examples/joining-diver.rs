
use std::{
    net::SocketAddr, str::FromStr,
    sync::{Arc, Mutex}, num::NonZeroU8, path::PathBuf,
};
use foca::Config;
use holydiver::swim::{core::{FocaRuntimeConfig, HolyDiverDataHandler, HolyDiverController}, types::ID, foca::setup_foca, server::host_server};
use dotenv::dotenv;

use anyhow::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    dotenv().ok();
    env_logger::init();
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
    data_dir.push("./examples/data2");
    let bind_addr = SocketAddr::from_str("127.0.0.1:9001")?;
    let identity = ID::new(bind_addr);
    let announce_to = Some(ID::new(SocketAddr::from_str("127.0.0.1:9000")?));
    let runtime_config = FocaRuntimeConfig {
        identity: identity.clone(),
        data_dir: data_dir,
        bind_addr: bind_addr,
        announce_to: announce_to,
        foca_config: foca_config,
    };
    let data_handler = Arc::from(Mutex::from(HolyDiverDataHandler::new(&runtime_config.data_dir, identity.clone())));
    let foca_command_sender = setup_foca(runtime_config, Box::new(data_handler.clone())).await?;

    let rest_controller = Arc::from(Mutex::from(HolyDiverController{
        foca_command_sender: foca_command_sender.clone(),
        data_handler: data_handler,
    }));
    host_server(9091, rest_controller).await?;
    Ok(())
}