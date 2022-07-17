use crate::{prelude::*, model::*, command::*, repo::Repo};

impl Repo {
    pub fn exec(&self, cmd: Command) -> anyhow::Result<()> {
        let Command { ts, author, inner } = cmd;
        match inner {
            CommandInner::Commit(CCommit { comment, branch, prev, rev: crev }) => {
                // TODO prem check
                let prev = hex_to_hash(prev)?;
                assert_eq!(self.get_ref(&branch)?, prev);
                let mut rev = Vec::new();
                for CRev { inner, object_kind, path } in crev {
                    let inner = match inner {
                        CRevInner::Update { content } => {
                            let hash = match object_kind {
                                ObjectKind::Data => self.add_data_object(content)?,
                                ObjectKind::Page => self.add_page_object(json_to_string(content)?)?,
                            };
                            RevInner::Update { hash }
                        },
                        CRevInner::Remove => RevInner::Remove,
                    };
                    rev.push(Rev { inner, object_kind, path });
                }
                self.commit(Commit { prev, ts, author, comment, merge: None, rev }, vec![&branch])?;
            },
            CommandInner::CreateCommonBranch(CCreateCommonBranch { prev }) => {
                self.create_branch(&Branch::Common(CommonBranch { ts, author }), hex_to_hash(prev)?)?;
            },
            CommandInner::MergeBranch(CMergeBranch { from, to, comment }) => {
                // TODO prem check
                let prev = self.get_ref(&from)?;
                let commit = if self.get_ref(&to)? == self.get_root_ref(&from)? {
                    // fast-forward
                    Commit { prev, ts, author, comment, merge: Some(from.clone()), rev: Vec::new() }
                } else {
                    // 3-way
                    unimplemented!()
                };
                self.commit(commit, vec![&to])?;
            }
        }
        Ok(())
    }
}
