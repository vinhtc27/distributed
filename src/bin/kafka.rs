use distributed::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{borrow::BorrowMut, collections::HashMap, io::StdoutLock, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Gossip,
}

enum InjectedPayload {
    Gossip,
}

#[allow(dead_code)]
struct KafkaNode {
    node: String,
    id: usize,
    messages: HashMap<String, Vec<usize>>,
    counter: HashMap<String, usize>,
    commited_offsets: HashMap<String, usize>,
    others: Vec<String>,
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || {
            // generate gossip events
            // TODO: handle EOF signal
            loop {
                std::thread::sleep(Duration::from_millis(300));
                if tx.send(Event::Injected(InjectedPayload::Gossip)).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            node: init.node_id.clone(),
            id: 1,
            messages: HashMap::new(),
            counter: HashMap::new(),
            commited_offsets: HashMap::new(),
            others: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {}
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Gossip => {}
                    Payload::Send { key, msg } => {
                        self.messages
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(msg);
                        self.counter.entry(key.clone()).or_insert(0);
                        reply.body.payload = Payload::SendOk {
                            offset: *self.counter.get(&key).unwrap(),
                        };
                        reply.send(&mut *output).context("reply to send")?;
                        *self.counter.get_mut(&key).unwrap() += 1;
                    }
                    Payload::Poll { offsets } => {
                        let msgs: HashMap<String, Vec<(usize, usize)>> = offsets
                            .iter()
                            .filter_map(|(key, offset)| {
                                self.messages.get(key).map(|key_msgs| {
                                    (
                                        key.clone(),
                                        key_msgs[*offset..]
                                            .iter()
                                            .enumerate()
                                            .map(|(i, m)| (offset + i, m.clone()))
                                            .collect(),
                                    )
                                })
                            })
                            .collect();
                        reply.body.payload = Payload::PollOk { msgs };
                        reply.send(&mut *output).context("reply to poll")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        for (key, offset) in offsets {
                            *self.commited_offsets.entry(key).or_insert(0).borrow_mut() = offset;
                        }
                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(&mut *output)
                            .context("reply to commit_offsets")?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        reply.body.payload = Payload::ListCommittedOffsetsOk {
                            offsets: keys
                                .iter()
                                .map(|key| {
                                    (key.clone(), *self.commited_offsets.get(key).unwrap_or(&0))
                                })
                                .collect(),
                        };
                        reply
                            .send(&mut *output)
                            .context("reply to list_committed_offsets")?;
                    }
                    Payload::SendOk { .. }
                    | Payload::PollOk { .. }
                    | Payload::CommitOffsetsOk
                    | Payload::ListCommittedOffsetsOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaNode, _, _>(())
}
