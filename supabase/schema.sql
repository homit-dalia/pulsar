-- Run once in the Supabase SQL editor. Creates a login role that can only use its own
-- schema "agent". The bridge creates its tables there on boot (supabase/tables.sql), and the
-- agents add any others they need.

create role agent_rw with login password 'REPLACE-WITH-A-LONG-RANDOM-PASSWORD';
grant agent_rw to postgres;  -- lets this session create objects owned by agent_rw
create schema agent authorization agent_rw;
