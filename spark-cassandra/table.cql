CREATE KEYSPACE IF NOT EXISTS sparkcassandrakeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
 
CREATE TABLE IF NOT EXISTS sparkcassandrakeyspace.userdata (id uuid PRIMARY KEY,username text,email text,attributes list<frozen<tuple<text, text, text, text>>>);