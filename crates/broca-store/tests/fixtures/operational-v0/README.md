# Pre-operational-projection database

`broca.db` was produced by the actual `broca-store` at commit
`446f9fb7ead593937b83af2687756777516aba0d`, not by dropping tables from a newer
database. Its unchanged, fully indexed `debt.wal` has four records: RunStarted,
ToolCallDeferred, a not-dispatched withdrawal terminal, and RunFinished. The run
is `upgrade-run`, bound to `("/upgrade-fixture", "runner", "owed")`. All data is
synthetic. The database contains neither `withdrawal_call` nor actor debt hints.

The generator used that revision's public `FileWal` and `OverhaulDb::index` APIs
and checked `indexed_seq=4` and `indexed_end=2250`. Opening this fixture with a
new operational projection must not mistake its valid old checkpoint for proof
that the new fold is complete.

SHA-256:

- `broca.db`: `aa7e0e42415832721e93593a3ad889db2a9294ce67ee6885df6b43aafcaed902`
- `debt.wal`: `085a51301cd975726e7f48104f0229c0131bf1c8e8582f159c3dc0c9e63e72cf`
