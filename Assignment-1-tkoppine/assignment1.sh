# Database Connection

DB_USR="postgres"
DB_PWD="postgres"
DB_NAME="postgres"
DB_HOST="127.0.0.1"
DB_PORT="5432"

export PGPASSWORD=$DB_PWD

# PostgreSQL database connection
#table creation
psql -h $DB_HOST -p $DB_PORT -U $DB_USR -d $DB_NAME -f create_tables.sql
#relations
psql -h $DB_HOST -p $DB_PORT -U $DB_USR -d $DB_NAME -f create_relations.sql
#queries
psql -h $DB_HOST -p $DB_PORT -U $DB_USR -d $DB_NAME -f queries.sql