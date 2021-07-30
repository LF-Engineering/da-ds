begin;
update ignore identities set name = concat(substring_index(trim(both '@' from trim(email)), '@', 1), '-MISSING-NAME') where (name = '' or name is null) and not (email = '' or email is null);
update ignore identities set name = concat(substring_index(trim(both '@' from trim(username)), '@', 1), '-MISSING-NAME') where (name = '' or name is null) and not (username = '' or username is null);
update ignore identities set name = concat(substring_index(trim(both '@' from trim(name)), '@', 1), '-REDACTED-EMAIL') where instr(trim(both '@' from trim(name)), '@') > 1;
update ignore identities set username = concat(substring_index(trim(both '@' from trim(username)), '@', 1), '-REDACTED-EMAIL') where instr(trim(both '@' from trim(username)), '@') > 1;
update ignore profiles p, identities i set p.name = i.name where p.uuid = i.uuid and (p.name is null or p.name = '') and not (i.name = '' or i.name is null);
commit;
