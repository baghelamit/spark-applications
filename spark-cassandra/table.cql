CREATE KEYSPACE IF NOT EXISTS sparkcassandrakeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
 
CREATE TABLE IF NOT EXISTS sparkcassandrakeyspace.user (id text PRIMARY KEY,username text,email text);
