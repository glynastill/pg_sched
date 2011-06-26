#PostgreSQL Scheduler

Simple scheduler for running stored functions against databases in a 
PostgreSQL cluster, with support for selective execution based on the 
replication role of nodes in Slony-I clusters.

The script is designed to be run once a minute from cron or similar, and
as such has a maximum resolution of 1 minute.  It provides very simple 
scheduling functionality to execute individual stored functions at 
specified intervals and offsets (E.g. The most complex schedule would 
be of the form "once every 5 hours at 20 minutes past the hour").

If you require more complex schedules or multi-step tasks try 
<a href="http://www.postgresql.org/ftp/pgadmin3/release/pgagent/" target="_blank">pgAgent</a>

## Command line

```bash
$ pg_sched.pl -h <db host> -p <db port> -d <db name> -U <db user> -n <schema> -t <table> -cl <slony clustername> -m <master sets> -l <lock file>
    -h      Hostname of database containing schedule table
            DEFAULT = localhost
    -p      Listening port of database containing schedule table
            DEFAULT = 5432
    -d      Name of database containing schedule table
            DEFAULT = postgres
    -U      User to connect to databse with, also used as default user for all tasks
            DEFAULT = pg_sched
    -n      Name of schema containing schedule table
            DEFAULT = public
    -t      Name of schedule table
            DEFAULT = pg_sched
    -cl     Name of slony cluster. 'auto' to autodetect, 'off' to disable
            DEFAULT = auto
    -m      Comma separated list of slony sets on the master. 'all' for all
            DEFAULT = all
    -l      Lockfile used to prevent concurrent execution of tasks.
            DEFAULT = not used
```

##Example usage

Create a PostgreSQL user for schedule to run as, to run tasks under 
different roles create the user as a superuser:

```postgresql
mydb=> CREATE USER pg_sched WITH PASSWORD 'my_password' SUPERUSER ; 
```

Create a pgpass file for the OS user that will run the script from cron:

```bash
$ echo *:*:*:pg_sched:my_password >> /home/myuser/.pgpass
chmod 600 /home/myuser/.pgpass
```

Create the schedule table in your database of choice:

```bash
$ psql -U pg_sched -d mydb -f pg_sched.sql
```

Add an entry to run from cron something like:

```cron
	* * * * * myuser /home/myuser/pg_sched.pl -d mydb -U pg_sched >> /var/log/pg_sched.log 2>&1
```

Insert a row into the table to create a task, e.g:

```postgresql
mydb=> INSERT INTO pg_sched (usename, datname, pronamespace, proname, proargs, proargtypes, enabled, frequency, frequency_offset) 
VALUES ('test_user', 'mydb', 'pg_catalog', 'date_part', '{"hour", "2015-03-25 13:50:59"}', '{text, timestamp}', 'A', '1 hour', '1 minute');
```
