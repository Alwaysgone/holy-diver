mod swim;

use std::{
    net::SocketAddr, str::FromStr,
    sync::{Arc, Mutex}, num::NonZeroU8, path::PathBuf,
};
use clap::{arg, Command, value_parser, builder::{NonEmptyStringValueParser, BoolValueParser, OsStr}};
use foca::Config;
use tokio::sync::mpsc::Sender;
use log::info;
use dotenv::dotenv;

use swim::{foca::FocaCommand, broadcast::DataHandler};
use swim::types::ID;

use swim::broadcast::{MessageType::FullSync, GossipMessage, Tag::SyncOperation};

use automerge::{transaction::Transactable, ObjType, ROOT, AutoCommit};
use uuid::Uuid;

use swim::{foca::FocaCommand::SendBroadcast, foca::setup_foca, core::FocaRuntimeConfig, server::host_server};
use anyhow::Result;

use crate::swim::core::MyDataHandler;

fn cli() -> Command {
    Command::new("holy-diver")
        .about("You expected SWIM but it was me DIO!")
        .arg_required_else_help(false)
        .args(&[
        arg!(--"bind-address" <BIND_ADDRESS> "Socket address to bind to. Example: 127.0.0.1:8080")
        .value_parser(NonEmptyStringValueParser::new())
        .id("bind-address"),
        arg!(identity: -i --identity <IDENTITY> "The address cluster members will use to talk to you. Defaults to bind-address")
        .value_parser(NonEmptyStringValueParser::new()),
        arg!(-a --"announce-to" <ANNOUNCE_TO> "Address to another holy-diver instance to join with")
        .value_parser(NonEmptyStringValueParser::new())
        .id("announce-to"),
        arg!(-d --"data-dir" <DATA_DIR> "Name of the file that will contain all active members")
        .value_parser(value_parser!(PathBuf))
        .default_value(OsStr::from("./data"))
        .id("data-dir"),
        arg!(-b --broadcast <BROADCAST> "Flag that indicates whether a broadcast should be sent on startup or not")
        .value_parser(BoolValueParser::new())
        .id("broadcast"),
        arg!(-p --port <REST_PORT> "Port for the REST endpoint")
        .value_parser(value_parser!(u16).range(1..))
        .default_value(OsStr::from("9090"))
        .id("rest-port")
        ])
        
}

fn get_test_data() -> AutoCommit {
    let mut state = AutoCommit::new();
    let values = state.put_object(ROOT, "values", ObjType::Map).unwrap();
    state.put(&values, "name", "dio").unwrap();
    state
}

fn get_broadcast_data() -> Vec<u8> {
    let mut data = get_test_data();
    data.save()
}

pub struct HolyDiverController {
    foca_command_sender: Sender<FocaCommand>,
    data_handler: Arc<Mutex<MyDataHandler>>,
}

impl HolyDiverController {
    fn get_field(&self, field_name: String) -> Option<String> {
        self.data_handler.lock().unwrap().get_field(field_name)
    }

    async fn set_field(&mut self, field_name: String, field_value: String) -> Result<()> {
        let mut handler = self.data_handler.lock().unwrap();
        handler.set_field(field_name, field_value).await?;
        // broadcasting the change so that all nodes get this update
        self.foca_command_sender.send(FocaCommand::SendBroadcast((SyncOperation {
            operation_id: Uuid::new_v4()
        }, GossipMessage::new(FullSync, handler.get_state())))).await?;
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    dotenv().ok();
    env_logger::init();
    let matches = cli().get_matches();
    info!("Starting with matches: {:?}", matches);
    
    let bind_addr = matches.get_one::<String>("bind-address")
    .map(|ba| SocketAddr::from_str(ba.as_str()).unwrap_or_else(|_| panic!("could not parse binding address as SocketAddr '{}'", ba)))
    .unwrap_or(SocketAddr::from_str("127.0.0.1:9000").unwrap());
    info!("Binding to {}", bind_addr);

    let identity = matches.get_one::<String>("identity")
    .map(|id| SocketAddr::from_str(id.as_str()).unwrap_or_else(|_| panic!("could not parse identity as SocketAddr '{}'", id)))
    .map(ID::new)
    .unwrap_or(ID::new(bind_addr));
    info!("Using identity {}", bind_addr); 

    let announce_to = matches.get_one::<String>("announce-to")
    .map(|a| SocketAddr::from_str(a.as_str()).unwrap_or_else(|_| panic!("could not parse announce-to as SocketAddr '{}'", a)))
    .map(ID::new);
    if announce_to.is_some() {
        info!("Announcing to {:?}", announce_to.clone().unwrap());
    } else {
        info!("Starting up as single swimmer");
    }

    let data_dir = matches.get_one::<PathBuf>("data-dir")
    .expect("clap should have provided a default value for data-dir");
    info!("Using {} as data dir", data_dir.display());

    let rest_port = matches.get_one::<u16>("rest-port")
    .expect("clap should have provided a default value for rest-port");
    info!("Using {} as rest port", rest_port);

    let should_broadcast = matches.get_one::<bool>("broadcast")
    .unwrap_or(&false)
    .to_owned();
    
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
    let runtime_config = FocaRuntimeConfig {
        identity,
        data_dir: data_dir.to_owned(),
        bind_addr,
        announce_to,
        foca_config,
    };
    // let state = read_state_from_disk(data_dir);
    // let state_ref = Arc::from(Mutex::from(state));
    let data_handler = Arc::from(Mutex::from(MyDataHandler::new(&runtime_config.data_dir)));
    let foca_command_sender = setup_foca(runtime_config, Box::new(data_handler.clone())).await?;
    if should_broadcast {
        let broadcast_data = get_broadcast_data();
        foca_command_sender.send(SendBroadcast((SyncOperation {
            operation_id: Uuid::new_v4()
        }, GossipMessage::new(FullSync, broadcast_data)))).await?;
    }
    let rest_controller = Arc::from(Mutex::from(HolyDiverController{
        foca_command_sender: foca_command_sender.clone(),
        data_handler: data_handler,
    }));
    host_server(rest_port.to_owned(), rest_controller).await?;
    Ok(())
}
