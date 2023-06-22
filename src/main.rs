use futures::StreamExt;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use std::{env, time::Duration};
use tokio_postgres::{
    config::ReplicationMode, replication::LogicalReplicationStream, types::PgLsn, Client, Config,
    NoTls, SimpleQueryMessage, SimpleQueryRow,
};

#[tokio::main]
async fn main() {
    let mut config = Config::new();
    config
        .user("tekul")
        .dbname(&env::var("DB_NAME").expect("DB_NAME not set"))
        .host("127.0.0.1")
        .replication_mode(ReplicationMode::Logical);

    let (client, connection) = config.connect(NoTls).await.unwrap();

    tokio::spawn(async move { connection.await });

    let _ = client.simple_query("DROP_REPLICATION_SLOT test_slot").await;
    println!("Dropped existing slot (if any)");

    let mut lsn: PgLsn = simple_query(
        &client,
        "CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput",
    )
    .await[0]
        .get("consistent_point")
        .unwrap()
        .parse()
        .unwrap();
    println!("lsn: {lsn}");
    println!("Starting replication stream...");

    // Use 'create publication all_tables for all tables;' from psql to create the publication we
    // want to stream replication messages from.
    let query = format!(
        r#"START_REPLICATION SLOT test_slot LOGICAL {lsn} ("proto_version" '1', "publication_names" 'all_tables')"#
    );
    let stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();
    let replication_stream = LogicalReplicationStream::new(stream);

    fn pg_timestamp() -> i64 {
        (std::time::UNIX_EPOCH + Duration::from_secs(946_684_800))
            .elapsed()
            .unwrap()
            .as_micros()
            .try_into()
            .unwrap()
    }

    tokio::pin!(replication_stream);

    while let Some(msg) = replication_stream.next().await {
        println!("{msg:?}");
        match msg {
            Ok(ReplicationMessage::PrimaryKeepAlive(k)) => {
                if k.reply() == 1 {
                    let _ = replication_stream
                        .as_mut()
                        .standby_status_update(lsn, lsn, lsn, pg_timestamp(), 0)
                        .await;
                }
            }
            Ok(ReplicationMessage::XLogData(body)) => match body.data() {
                LogicalReplicationMessage::Begin(_) => {}
                LogicalReplicationMessage::Commit(body) => {
                    lsn = body.end_lsn().into();
                }
                LogicalReplicationMessage::Origin(_) => todo!(),
                LogicalReplicationMessage::Relation(_) => {}
                LogicalReplicationMessage::Type(body) => {
                    print!("{body:?}");
                },
                LogicalReplicationMessage::Insert(body) => {
                    let data = body.tuple();
                    print!("{data:?}");
                }
                LogicalReplicationMessage::Update(body) => {
                    let data = body.new_tuple();
                    print!("{data:?}");
                },
                LogicalReplicationMessage::Delete(_) => todo!(),
                LogicalReplicationMessage::Truncate(_) => todo!(),
                _ => todo!(),
            },
            Ok(_) => todo!(),
            Err(e) => {
                eprintln!("{e:?}");
            }
        }
    }
}

async fn simple_query(client: &Client, query: &str) -> Vec<SimpleQueryRow> {
    let msgs = client.simple_query(query).await.unwrap();
    msgs.into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}
