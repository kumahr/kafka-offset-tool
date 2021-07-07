use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::process::Command;

#[derive(Serialize, Deserialize)]
struct Topic {
    topic: String,
    group: String,
    date: String,
    offset: i64,
    partitions: Vec<i64>,
    reset_mode: String,
}

#[derive(Serialize, Deserialize)]
struct Topics {
    env: String,
    topics: Vec<Topic>,
}

fn get_options() -> HashMap<String, String> {
    let mut options = HashMap::new();
    options.insert(String::from("EARLIEST"), String::from("--to-earliest"));
    options.insert(String::from("LATEST"), String::from("---to-latest"));
    options.insert(String::from("DATETIME"), String::from("--to-datetime"));
    options.insert(String::from("OFFSET"), String::from("--to-offset"));
    options
}

fn build_command(env: &String, topic: &Topic, broker: &String) -> Option<String> {
    let options = get_options();
    let reset_mode = match options.get(&topic.reset_mode) {
        Some(o) => o,
        None => return None,
    };
    let mut offset = String::from("");
    let mut datetime = String::from("");
    let mut topic_value = topic.topic.clone();
    let mut all_topics = String::from("--all-topics");
    match reset_mode.as_str() {
        "--to-offset" => {
            offset = String::from(topic.offset.to_string());
        }
        "--to-datetime" => {
            datetime = String::from(&topic.date);
        }
        _ => {}
    }
    if topic.partitions.len() > 0 {
        all_topics = String::from("--topic");
        let joined_partitions = topic.partitions.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(",");
        topic_value = format!("{}:{}", topic_value, joined_partitions);
    }
    let secu_file = format!("secu.{}.config", env.to_lowercase());
    return Some(format!("kafka-consumer-groups --bootstrap-server {} --group {} --reset-offsets {} {} {} {} {} --execute --command-config /opt/secrets/{}", broker, topic.group, reset_mode, offset, datetime, all_topics, topic_value, secu_file));
}

fn reset_consumer_group(
    env: &String,
    topic: &Topic,
    broker: &String,
) -> Result<std::process::Output, String> {
    let result = match build_command(env, &topic, broker) {
        Some(c) => c,
        None => return Err(String::from("Command could not be built!")),
    };
    println!("{}", result);
    let args = result.split(" ").collect::<Vec<&str>>();
    let output;
    if cfg!(target_os = "windows") {
        output = Command::new("cmd")
            .arg("/C")
            .args(&args)
            .output();
    } else {
        output = Command::new(args[0])
            .args(&args[1..args.len()])
            .output();
    };
    match output {
        Ok(sout) => return Ok(sout),
        Err(e) => {
            return Err(String::from(format!("{}", e)));
        }
    };
}

fn main() {
    let topics_file = fs::read_to_string("topics.json").unwrap();
    let topics: Topics = serde_json::from_str(&topics_file).unwrap();
    let broker = env::var(format!("BROKERS_{}", topics.env)).unwrap();
    topics.topics.iter().for_each(|topic| {
        match reset_consumer_group(&topics.env, topic, &broker) {
            Ok(x) => {
                if x.status.success() {
                    if x.stderr.len() > 0 {
                        println!("{}", String::from_utf8(x.stderr).unwrap())
                    } else {
                        println!("{}", String::from_utf8(x.stdout).unwrap())
                    }
                } else {
                    println!("The command does not exist!")
                }
            }
            Err(e) => println!("{}", e),
        }
    });
}
