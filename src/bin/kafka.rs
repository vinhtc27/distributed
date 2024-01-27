use distributed::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{borrow::BorrowMut, collections::HashMap, io::StdoutLock};

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
    GossipSend {
        key: String,
        msg: usize,
    },
    GossipCommit {
        offsets: HashMap<String, usize>,
    },
}

enum InjectedPayload {
    GossipSend { key: String, msg: usize },
    GossipCommit { offsets: HashMap<String, usize> },
}

#[allow(dead_code)]
struct KafkaNode {
    node: String,
    id: usize,
    messages: HashMap<String, Vec<usize>>,
    counter: HashMap<String, usize>,
    commited_offsets: HashMap<String, usize>,
    others: Vec<String>,
    tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
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
            tx,
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
                InjectedPayload::GossipSend { key, msg } => {
                    for n in &self.others {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::GossipSend {
                                    key: key.clone(),
                                    msg,
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
                InjectedPayload::GossipCommit { offsets } => {
                    for n in &self.others {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::GossipCommit {
                                    offsets: offsets.clone(),
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::GossipSend { key, msg } => {
                        self.messages
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(msg);
                        *self.counter.entry(key.clone()).or_insert(0).borrow_mut() += 1;
                    }
                    Payload::GossipCommit { offsets } => {
                        for (key, offset) in offsets {
                            *self.commited_offsets.entry(key).or_insert(0).borrow_mut() = offset;
                        }
                    }
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
                        self.tx
                            .send(Event::Injected(InjectedPayload::GossipSend { key, msg }))?;
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
                        for (key, offset) in offsets.clone() {
                            *self.commited_offsets.entry(key).or_insert(0).borrow_mut() = offset;
                        }
                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(&mut *output)
                            .context("reply to commit_offsets")?;
                        self.tx
                            .send(Event::Injected(InjectedPayload::GossipCommit { offsets }))?;
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
