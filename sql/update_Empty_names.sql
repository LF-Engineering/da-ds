update ignore identities set name = concat(substring_index(trim(email), '@', 1), '-MISSING-NAME') where (name = '' or name is null) and not (email = '' or email is null);
update ignore identities set name = concat(substring_index(trim(leading '@' from trim(username)), '@', 1), '-MISSING-NAME') where (name = '' or name is null) and not (username = '' or username is null);
