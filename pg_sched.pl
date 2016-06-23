#!/usr/bin/perl

# Script: pg_sched.pl
# Copyright: 26/06/2011: v1.0.0 Glyn Astill <glyn@8kb.co.uk>
# PostgreSQL 9.0+ (optional Slony-I)
#
# This script is a command-line utility to schedule execution of stored
# functions against databases in a PostgreSQL cluster, with support for 
# selective execution based on the replication role of nodes in Slony-I 
# clusters.
#
# The script is designed to be run from cron and has a maximum resolution
# of 1 minute. 
#
# This script is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This script is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this script. If not, see <http://www.gnu.org/licenses/>.

use strict;
use warnings;
use threads;
use Time::HiRes qw/gettimeofday/;
use DBI;
use sigtrap 'handler' => \&cleanExit, 'HUP', 'INT','ABRT','QUIT','TERM';
use Getopt::Long qw/GetOptions/;
Getopt::Long::Configure('no_ignore_case');
use vars qw{%opt};

use constant false => 0;
use constant true  => 1;

my $g_slony_cluster = "test_replication";
my $g_dbhost = 'localhost';
my $g_dbport = 5432;
my $g_dbname = 'postgres';
my $g_dbuser = 'pg_sched';
my $g_dbschema = 'public';
my $g_dbtable = 'pg_sched';
my $g_clname = 'auto';
my $g_sets = 'all';
my $g_usage = "pg_sched.pl -h <db host> -p <db port> -d <db name> -U <db user> -n <schema> -t <table> -cl <slony clustername> -m <master sets> -w <lock file> -l <logfile>
    -h      Hostname of database containing schedule table
            DEFAULT = $g_dbhost
    -p      Listening port of database containing schedule table 
            DEFAULT = $g_dbport
    -d      Name of database containing schedule table
            DEFAULT = $g_dbname
    -U      User to connect to databse with, also used as default user for all tasks
            DEFAULT = $g_dbuser
    -n      Name of schema containing schedule table
            DEFAULT = $g_dbschema
    -t      Name of schedule table
            DEFAULT = $g_dbtable
    -cl     Name of slony cluster. 'auto' to autodetect, 'off' to disable
            DEFAULT = $g_clname
    -m      Comma separated list of slony sets on the master. 'all' for all
            DEFAULT = $g_sets
    -w      Lockfile used to prevent concurrent execution of tasks.
            DEFAULT = not used
    -l      File to log to.
            DEFAULT = log to STDOUT instead
";

my @g_databases;
my $g_origin = false;
my @g_pronames;
my $g_errors;
my $g_lockfile;
my $g_logfile;

die $g_usage unless GetOptions(\%opt, 'host|H=s', 'port|p=i', 'dbname|d=s', 'user|U=s', 'schema|n=s', 'table|t=s', 'lockfile|w=s', 'clname|cl=s', 'msets|m=s', 'logfile|l=s');

if (defined($opt{host})) {
    $g_dbhost = $opt{host};
}
if (defined($opt{port})) {
    $g_dbport = $opt{port};
}
if (defined($opt{dbname})) {
    $g_dbname = $opt{dbname};
}
if (defined($opt{user})) {
    $g_dbuser = $opt{user};
}
if (defined($opt{schema})) {
    $g_dbschema = $opt{schema};
}
if (defined($opt{table})) {
    $g_dbtable = $opt{table};
}
if (defined($opt{lockfile})) {
    $g_lockfile = $opt{lockfile};
}
if (defined($opt{clname})) {
    $g_clname = $opt{clname};
}
if (defined($opt{msets})) {
    $g_sets = $opt{msets};
}
if (defined($opt{logfile})) {
    $g_logfile = $opt{logfile};
}

# If lockfile supplied check if the script is already running
if (defined($g_lockfile)) {
    schedLock($g_lockfile, 'lock');
}

# Get a list of databases in the postgresql cluster
@g_databases = loadDatabases($g_dbhost, $g_dbport, $g_dbname, $g_dbuser);

# For each database get its replication status then get any work to do
undef @g_pronames;
foreach my $target_dbname (@g_databases) {
    if ($g_clname ne 'off') {
        $g_origin = checkSlony($g_dbhost, $g_dbport, $target_dbname, $g_dbuser, $g_clname, $g_sets);
    }
    else {
        $g_origin = true;
    }
    @g_pronames = (@g_pronames, loadWork($g_dbhost, $g_dbport, $g_dbname, $g_dbuser, $g_dbschema, $g_dbtable, $g_origin, $target_dbname));
}
# Do all the work we have collected for all databases
$g_errors = doWork(\@g_pronames, $g_dbhost, $g_dbport, $g_dbname, $g_dbuser, $g_dbschema, $g_dbtable, $g_logfile);

if ($g_errors > 0) {
    warn ("ERROR: Encountered $g_errors errors processing the schedule\n") if $@;
}

cleanExit($g_lockfile);

sub cleanExit{
    my $lockfile = shift;

    if (defined($lockfile)) {
        schedLock($lockfile, 'unlock');
    }
    exit(0);
}

sub schedLock{
    my $lockdir = shift;
    my $op = shift;

    if ($op eq 'lock') {
        eval {
            mkdir($lockdir) or die "Can't make directory: $!";
        };
        die ("Script already running") if $@;
    }
    else {
        eval {
            rmdir $lockdir;
        };
        die ("unable to remove lock\n") if $@;
    }
}

sub logFile {
    my $logfile = shift;
    my $message = shift;

    eval {
        open(LOGFILE, ">>", $logfile);
        flock(LOGFILE, 2);
        print LOGFILE $message;
        close (LOGFILE);
    };
    warn ("WARNING: unable to log to file $logfile\n") if $@;
}

sub loadDatabases{
    my $dbhost = shift;
    my $dbport = shift;
    my $dbname = shift;
    my $dbuser = shift;
    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my @databases;

    $dsn = "DBI:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser);
        {
            $dbh->do("SET application_name = 'pg_sched'");
            
            $query = "SELECT datname FROM pg_catalog.pg_database WHERE datallowconn AND NOT datistemplate;";
        
            $sth = $dbh->prepare($query);
            $sth->execute();

            while (my $databasename = $sth->fetchrow) {
                push(@databases,  $databasename);
            }
        }
    };
    warn ("ERROR: Loading databaees failed\n") if $@;
    
    return @databases;
}

sub checkSlony{
    my $dbhost = shift;
    my $dbport = shift;
    my $target_dbname = shift;
    my $dbuser = shift;
    my $clname = shift;
    my $sets = shift;
    my $qw_clname;
    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $local_sets;
    my $all_sets;
    my $run_master = false;
    
    $dsn = "DBI:Pg:dbname=$target_dbname;host=$dbhost;port=$dbport;";
    eval {
        $dbh = DBI->connect($dsn, $dbuser);
        {
            $dbh->do("SET application_name = 'pg_sched'");

            if ($clname ne 'auto') {           
                $query = "SELECT substr(n.nspname,2) FROM pg_catalog.pg_namespace n WHERE n.nspname = ?;";
            }
            else {
                $query = "SELECT substr(n.nspname,2) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                          WHERE c.relname = 'sl_set' AND c.relkind = 'r' GROUP BY n.nspname;";
            }
        
            $sth = $dbh->prepare($query);

            if ($clname ne 'auto') {
                $sth->bind_param(1, "_" . $clname);
            }

            $sth->execute();
            $clname = $sth->fetchrow;

            if (defined($clname)) {
                $qw_clname = $dbh->quote_identifier("_" . $clname);

                $query = "SELECT count(*), sum(CASE WHEN s.set_origin = $qw_clname.getlocalnodeid(?::name) THEN 1 ELSE 0 END)
                        FROM $qw_clname.sl_set s" . (($sets ne 'all')?' WHERE s.set_id = ANY(?::integer[]);':';');

                $sth = $dbh->prepare($query);
                $sth->bind_param(1, "_" . $clname);
                if ($sets ne 'all') {
                    $sth->bind_param(2, [ split(',', $sets) ]);
                }
            
                $sth->execute();

                ($all_sets, $local_sets) = $sth->fetchrow;

                if (($all_sets // 0) == ($local_sets // 0)) {
                    $run_master = true;
                }
                else {
                    $run_master = false;
                }
            }
            else {
                $run_master = true;
            }
        }
    };
    warn ("ERROR: Failed reading slony node config\n") if $@;

    return $run_master;
}

sub loadWork{
    my $dbhost = shift;
    my $dbport = shift;
    my $dbname = shift;
    my $dbuser = shift;
    my $dbschema = shift;
    my $dbtable = shift;
    my $node_role = shift;
    my $target_dbname = shift;
    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_schedname;
    my @pronames;

    if ($node_role ne 'D') {    
        $dsn = "DBI:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
        eval {
            $dbh = DBI->connect($dsn, $dbuser);
            {
                $dbh->do("SET application_name = 'pg_sched'");

                $qw_schedname = $dbh->quote_identifier($dbschema) . '.' . $dbh->quote_identifier($dbtable);

                $query = "SELECT p.id, p.datname, p.pronamespace, p.proname, p.proargs, p.proargtypes, COALESCE(p.usename,current_user), ?::text, p.running FROM $qw_schedname p
                        WHERE p.datname = ? AND (p.enabled = 'A' OR p.enabled = ?)
                        AND (p.last_run IS NULL OR
                            (date_trunc('m', current_timestamp)-date_trunc('m',p.last_run))::interval >= p.frequency)
                        AND (p.isloose OR 
                            (extract(EPOCH FROM date_trunc('m', current_timestamp-coalesce(frequency_offset,'0s'::interval)))::integer
                            %extract(EPOCH FROM frequency)::integer) = 0);";
    
                $sth = $dbh->prepare($query);
                $sth->bind_param(1, ($node_role?'O':'R'));
                $sth->bind_param(2, $target_dbname);
                $sth->bind_param(3, ($node_role?'O':'R'));
                $sth->execute();             
    
                while (my @prodef = $sth->fetchrow) {
                    push(@pronames,  \@prodef);
                }
            }
        };
        warn ("ERROR: Loading work failed\n") if $@;
    }

    return @pronames;
}

sub doWork{
    my $pronames = shift;
    my $dbhost = shift;
    my $dbport = shift;
    my $dbname = shift;
    my $dbuser = shift;
    my $dbschema = shift;
    my $dbtable = shift;
    my $logfile = shift;
    my @workers;
    my @worker_result;
    my $bad = 0;

    foreach my $prodef (@$pronames) {
        my ($t) = threads->new(\&runWorker, $prodef, $dbhost, $dbport, $dbname, $dbuser, $dbschema, $dbtable, $logfile);
        push(@workers,$t);
    }
    foreach (@workers) {
        @worker_result = $_->join;
        if ($worker_result[0] == true) {
            print("$worker_result[1] [SUCCEEDED]\n") if (!defined($logfile));
        }
        else {
            print("$worker_result[1] [FAILED]\n") if (!defined($logfile));
            $bad++;
        }
    }
    return $bad;
}

sub runWorker{
    my $prodef = shift;
    my $dbhost = shift;
    my $dbport = shift;
    my $dbname = shift;
    my $dbuser = shift;
    my $dbschema = shift;
    my $dbtable = shift;
    my $logfile = shift;

    my $dsn;
    my $dbh;
    my $sth;
    my $query;
    my $qw_schedname;
    my @result;
    my $success;
    my ($g_year, $g_month, $g_day, $g_hour, $g_min, $g_sec) = (localtime(time))[5,4,3,2,1,0];
    my $timestamp = sprintf ("%02d/%02d/%04d %02d:%02d:%02d", $g_day, $g_month+1, $g_year+1900, $g_hour, $g_min, $g_sec);
    my $detail = $timestamp . " Schedule: $dbschema.$dbtable Task: id#@$prodef[0] @$prodef[1].@$prodef[2].@$prodef[3](" . 
                (defined(@$prodef[4])?join(', ',@{@$prodef[4]}):'') . ") Rep Role: @$prodef[7] User: @$prodef[6] Result: ";
    my $typindex = 0;
    my $start;
    my $end;
    
    if (!defined(@$prodef[8]) || !kill(0, @$prodef[8])) {
        eval {
            eval {
                $dsn = "DBI:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
                $dbh = DBI->connect($dsn, $dbuser);
                $dbh->do("SET application_name = 'pg_sched'");

                $qw_schedname = $dbh->quote_identifier($dbschema) . '.' . $dbh->quote_identifier($dbtable);

                $query = "UPDATE $qw_schedname SET running = ? WHERE id = ?;";
                $sth = $dbh->prepare($query);
                $sth->bind_param(1, $$);
                $sth->bind_param(2, @$prodef[0]);
                $sth->execute(); 
                $sth->finish;
                $dbh->disconnect();
            };
            if ($@) {
                warn ("WARNING: pre update of schedule failed\n");
            }
            			
            $dsn = "DBI:Pg:dbname=@$prodef[1];host=$dbhost;port=$dbport;";
            $dbh = DBI->connect($dsn, $dbuser);
            $dbh->do("SET application_name = 'pg_sched'");

            $sth = $dbh->prepare("SET session_authorization = ?");
            $sth->bind_param(1, @$prodef[6]);
            $sth->execute();

            $query = "SELECT " . $dbh->quote_identifier(@$prodef[2]) . '.' . $dbh->quote_identifier(@$prodef[3]) . '(';
            if (defined(@$prodef[4])) {
                foreach my $arg (@{@$prodef[4]}) {
                    #$query = $query . ($typindex>0?',':'') . $dbh->quote($arg) . '::' . @{@$prodef[5]}[$typindex];
                    $query .= ($typindex>0?',':'') . '?::' . @{@$prodef[5]}[$typindex];
                    $typindex++;
                }
            }
            $query .= ');';
         
            $sth = $dbh->prepare($query);

            $typindex = 0;
            if (defined(@$prodef[4])) {
                foreach my $arg (@{@$prodef[4]}) {
                    $typindex++;
                    $sth->bind_param($typindex, $arg);
                }
            }
            $start = gettimeofday();
            $sth->execute();
            $end = gettimeofday();

            while (my @result = $sth->fetchrow) {
                $success = true;
                $detail .= ($result[0] // 'NULL') . ' (Timing: ' . sprintf("%.2f sec", $end-$start) . ')';
            }
            $sth->finish;
            $dbh->disconnect();

            eval {
                $dsn = "DBI:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
                $dbh = DBI->connect($dsn, $dbuser);
                $dbh->do("SET application_name = 'pg_sched'");

                $qw_schedname = $dbh->quote_identifier($dbschema) . '.' . $dbh->quote_identifier($dbtable);

                $query = "UPDATE $qw_schedname SET running = NULL, last_run = ?::timestamp WHERE id = ?;";
                $sth = $dbh->prepare($query);
                $sth->bind_param(1, $timestamp);
                $sth->bind_param(2, @$prodef[0]);
                $sth->execute(); 
                $sth->finish;
                $dbh->disconnect();
            };
            if ($@) {
                warn ("WARNING: post update of schedule failed\n");
            }
        };
        if ($@) {
            $success = false;
            $detail = $@;
            warn ("WARNING: worker execution failed\n");
        }
    }
    else {
		$success = false;
		$detail .= 'already running PID ' . @$prodef[8];
    }

    if (defined($logfile)) {
       logFile($logfile, $detail . ($success?' [SUCCEEDED]':' [FAILED]') . "\n");
    }

    return ($success, $detail);
}

