-- Synthetic sessions only. Extra columns ensure extraction preserves the source schema.
CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT NOT NULL, directory TEXT NOT NULL, time_updated INTEGER NOT NULL);
CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, time_created INTEGER NOT NULL, data TEXT NOT NULL, extra BLOB);
CREATE INDEX message_session ON message(session_id, time_created);
CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL, time_created INTEGER NOT NULL, data TEXT NOT NULL);
CREATE INDEX part_session ON part(session_id, message_id);
INSERT INTO session VALUES ('ses_synthetic', 'Synthetic head', '/synthetic/project', 9000), ('ses_other', 'Not exported', '/synthetic/other', 10000);
INSERT INTO message VALUES
 ('msg_001', 'ses_synthetic', 1000, '{"role":"user"}', X'0102'),
 ('msg_002', 'ses_synthetic', 2000, '{"role":"assistant"}', NULL),
 ('msg_003', 'ses_synthetic', 3000, '{"role":"user"}', NULL),
 ('msg_004', 'ses_synthetic', 4000, '{"role":"assistant"}', NULL),
 ('msg_005', 'ses_synthetic', 5000, '{"role":"system"}', NULL),
 ('msg_006', 'ses_synthetic', 6000, '{"role":"user"}', NULL),
 ('msg_007', 'ses_synthetic', 7000, '{"role":"assistant","summary":true,"finish":"stop"}', NULL),
 ('msg_008', 'ses_synthetic', 8000, '{"role":"assistant","summary":true,"finish":"stop","error":{"name":"APIError"}}', NULL),
 ('msg_009', 'ses_synthetic', 9000, '{"role":"assistant","error":{"name":"APIError"}}', NULL),
 ('msg_other', 'ses_other', 10000, '{"role":"user"}', NULL);
INSERT INTO part VALUES
 ('prt_001', 'msg_001', 'ses_synthetic', 1000, '{"type":"text","text":"Look at this synthetic image."}'),
 ('prt_002', 'msg_001', 'ses_synthetic', 1001, '{"type":"text","text":"Ignored text","ignored":true}'),
 ('prt_003', 'msg_001', 'ses_synthetic', 1002, '{"type":"text","text":"Synthetic context","synthetic":true}'),
 ('prt_004', 'msg_001', 'ses_synthetic', 1003, '{"type":"file","mime":"image/png","url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="}'),
 ('prt_005', 'msg_002', 'ses_synthetic', 2000, '{"type":"text","text":"Reading."}'),
 ('prt_006', 'msg_002', 'ses_synthetic', 2001, '{"type":"reasoning","text":"Synthetic reasoning","metadata":{"anthropic":{"signature":"never-import-this"}}}'),
 ('prt_007', 'msg_002', 'ses_synthetic', 2002, '{"type":"tool","callID":"reused","tool":"read","state":{"status":"completed","input":{"path":"synthetic.txt"},"output":"Synthetic file content","time":{"start":2000,"end":2100},"attachments":[{"mime":"image/png","url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="}]}}'),
 ('prt_008', 'msg_003', 'ses_synthetic', 3000, '{"type":"text","text":"Read again."}'),
 ('prt_009', 'msg_004', 'ses_synthetic', 4000, '{"type":"tool","callID":"reused","tool":"read","state":{"status":"completed","input":{},"output":"Old content","time":{"compacted":4500},"attachments":[{"mime":"image/png","url":"file:///never-open"}]}}'),
 ('prt_010', 'msg_005', 'ses_synthetic', 5000, '{"type":"text","text":"Source system text"}'),
 ('prt_011', 'msg_006', 'ses_synthetic', 6000, '{"type":"compaction","auto":true}'),
 ('prt_012', 'msg_007', 'ses_synthetic', 7000, '{"type":"text","text":"Synthetic summary."}'),
 ('prt_013', 'msg_008', 'ses_synthetic', 8000, '{"type":"text","text":"Errored summary is not model history."}'),
 ('prt_014', 'msg_009', 'ses_synthetic', 9000, '{"type":"text","text":"Errored reply."}'),
 ('prt_other', 'msg_other', 'ses_other', 10000, '{"type":"text","text":"Never export this session."}');
