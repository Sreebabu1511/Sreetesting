#!/usr/bin/perl
use strict;
use warnings;
use File::Temp qw(tempfile);
use File::Spec;
use File::Basename;
use Sys::Hostname;
use FindBin qw($Bin);
use File::Path qw(make_path);
use POSIX qw(strftime);
use Time::HiRes qw(usleep);
use Cwd 'abs_path';
use Getopt::Long;
 
# Add the directory containing the logger module to @INC
use lib "$FindBin::Bin/commons";
use Logger qw(log_message set_logfile set_criticalinfofile generate_run_id);
use LockFile;
use DBQuery qw(execute_query);
 
my $VERSION = "1.0.0";
my $TODAY = strftime "%Y%m%d", localtime;
my $SCRIPT_DIR = $Bin;
my $DATA_DIR = File::Spec->catdir($SCRIPT_DIR, '..', 'data');
my $LOGDIR = abs_path(File::Spec->catdir($DATA_DIR, '..', 'log'));
 
my $LOCKFILE = File::Spec->catfile($SCRIPT_DIR, "D24_Check_Integrator_RecordStatus.lock");
my $is_windows = $^O eq 'MSWin32'; # Check if the OS is Windows 
my $lock;
 
# Timeout (set a default timeout, e.g., 10 seconds)
my $TIMEOUT = 10;
 
# If run_id is not provided, generate it dynamically
my ($run_id);
my $LOGFILE = File::Spec->catfile($LOGDIR, "D24_Check_Integrator_RecordStatus-${run_id}.log");
# Parse the command-line arguments using Getopt::Long
GetOptions(
    'run_id=s' => \$run_id  # Capture the run_id argument as a string
);
 
# Check if the run_id was provided and print it
if (defined $run_id) {
    print "Captured run_id: $run_id\n";
} else {
  $run_id = generate_run_id();
    print "Not received 'run_id' from Wrapper. Hence generating run_id dynamically. Run id is: $run_id\n";
}
 
# Define the CRITICALINFOFILE variable (make sure the path is correct for your needs)
my $CRITICALINFOFILE = File::Spec->catfile($LOGDIR, "D24_Check_Integrator_RecordStatus-CriticalInfo-${run_id}.log");
 
# Create the log directory if it does not exist
unless (-d $LOGDIR) {
    make_path($LOGDIR, { chmod => 0777 }) or do {
        log_message(Script_Type => 'DIAGNOSTICS', Data =>"Could not create log directory: $LOGDIR\n");
        exit 62;
    };
}
 
# Ensure the log file exists
unless (-e $LOGFILE) {
    open my $log_fh, '>', $LOGFILE or do {
        log_message(Script_Type => 'DIAGNOSTICS', Data =>"Could not create log file: $LOGFILE\n");
        exit 63;
    };
    close $log_fh;
}
 
unless (-e $CRITICALINFOFILE) {
    open my $critical_fh, '>', $CRITICALINFOFILE or do {
        log_message(Script_Type => 'CRITICALINFO', Data =>"Could not create log file: $CRITICALINFOFILE\n");
        exit 63;
    };
    close $critical_fh;
}
 
# Acquire a lock to prevent multiple script instances from running simultaneously
sub acquire_lock {
    my $lock = LockFile->new($LOCKFILE, \&log_message); # Create a lock object
    unless ($lock->acquire()) {
        log_message(Script_Type => 'CRITICALINFO', Data =>"Could not acquire lock. Another instance of the script may be running.");
    
        exit 69;
    }
    return $lock;
}
 
# Executes a query and returns the count of matching records
sub execute_query_internal {
    my ($query) = @_;
    my $output = execute_query($query);
    return $output =~ /---\s*\n\s*(\d+)/ ? $1 : 0; # Extract and return the count from the query result
}
 
# Executes a query and returns the first value from the result
sub execute_query_firstvalue {
    my ($query) = @_;
    return execute_query($query);
}
 
# Check if there are any stuck 'IC' transactions and verify if they are progressing
sub check_ic_stuck_transactions {
    my $query = "[SELECT COUNT(*) FROM sl_evt_data WHERE evt_stat_cd='IC']";
    my $initial_count = execute_query_internal($query);
    # If no records found, exit with an error message
    if ($initial_count == 0) {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "No records found for IC transactions. Exiting Script.");
    log_message(Script_Type => 'CRITICALINFOFILE', Data => "No records found for IC transactions. Exiting Script.");
        print("No records found for IC transactions. Exiting Script.\n");
        exit 3;
    }
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Initial IC Count: $initial_count");
    print("Initial count: $initial_count\n");
  log_message(Script_Type => 'CRITICALINFOFILE', Data => "Intial Count: $initial_count\n");
    usleep($TIMEOUT * 1_000_000); # Sleep for the timeout period (in microseconds)
    my $new_count = execute_query_internal($query);
    # Log and print the IC transaction count after the timeout period
    print("Count after $TIMEOUT seconds: $new_count\n");
  log_message(Script_Type => 'DIAGNOSTICS', Data => "IC count after $TIMEOUT seconds: $new_count");
  log_message(Script_Type => 'CRITICALINFOFILE', Data => "IC count after $TIMEOUT seconds: $new_count");
    return ($initial_count, $new_count);
}
 
# Handle the case when 'IC' transactions are stuck (not progressing)
sub handle_stuck_transactions {
    my ($initial_count, $new_count) = @_;
 
    if ($initial_count == $new_count) {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "No progress detected in IC transactions (initial: $initial_count, new: $new_count). Possible stuck transactions.");
        log_message(Script_Type => 'CRITICALINFOFILE', Data => "No progress detected in IC transactions (initial: $initial_count, new: $new_count). Possible stuck transactions.");
    print "No progress detected in IC transactions.\n";
    } else {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "IC transactions are progressing (initial: $initial_count, new: $new_count).");
    log_message(Script_Type => 'CRITICALINFOFILE', Data => "IC transactions are progressing (initial: $initial_count, new: $new_count).");
        print "IC transactions are progressing.\n";
    }
 
    # Existing logic for handling stuck transactions
    my $query = "[SELECT TO_CHAR(evt_dt, 'YYYY/MM/DD HH24:MI:SS') AS formatted_date, evt_id, evt_data_seq FROM sl_evt_data WHERE evt_stat_cd = 'IC' ORDER BY evt_dt]";
    my $event_data = execute_query_firstvalue($query);
 
    my $first_event_data;
    my $last_event_data;
 
    while ($event_data =~ /(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2})\s+(\S+)\s+(\d+)/g) {
        $first_event_data //= { formatted_date => $1, evt_data_seq => $3 };
        $last_event_data = { formatted_date => $1, evt_data_seq => $3 };
    }
 
    log_message(Script_Type => 'DIAGNOSTICS', Data => "First event: formatted_date: $first_event_data->{formatted_date}, evt_data_seq: $first_event_data->{evt_data_seq}");
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Last event: formatted_date: $last_event_data->{formatted_date}, evt_data_seq: $last_event_data->{evt_data_seq}");
  log_message(Script_Type => 'CRITICALINFOFILE', Data => "First event: formatted_date: $first_event_data->{formatted_date}, evt_data_seq: $first_event_data->{evt_data_seq}");
    log_message(Script_Type => 'CRITICALINFOFILE', Data => "Last event: formatted_date: $last_event_data->{formatted_date}, evt_data_seq: $last_event_data->{evt_data_seq}");
 
    print "First Event: formatted_date: $first_event_data->{formatted_date}, evt_data_seq: $first_event_data->{evt_data_seq}\n";
    print "Last Event: formatted_date: $last_event_data->{formatted_date}, evt_data_seq: $last_event_data->{evt_data_seq}\n";
}
 
# Main routine that runs the entire process
sub main {
    # Set the log file before calling log_message
    set_logfile($LOGFILE);
  set_criticalinfofile($LOGFILE);
  
    # New diagnostic logging messages
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Acquiring the lock before starting the script");
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Starting D24_Check_Integrator_RecordStatus, version $VERSION");
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Running on host: " . hostname());
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Script is being run as part of validation step after ZABBIX Alert");
    
    # Print message indicating that script execution has started
    print "Script Execution started \n";
 
    # Acquire the lock to prevent multiple instances of the script from running
    $lock = acquire_lock();
    
    # Continue with the rest of the script logic
    my ($initial_count, $new_count) = check_ic_stuck_transactions();
    handle_stuck_transactions($initial_count, $new_count);
    
    # Release the lock after the script finishes
    $lock->release();
    
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Script execution completed successfully");
    exit 0;
}
 
# Ensure the lock is released on early termination
END {
    if ($lock) {
        $lock->release();
        log_message(Script_Type => 'DIAGNOSTICS', Data => "Lock released in END block");
    }
}
 
main();
