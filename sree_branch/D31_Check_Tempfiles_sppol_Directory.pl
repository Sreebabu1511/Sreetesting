#!/usr/bin/perl
use strict;
use warnings;
use File::Find;
use File::stat;
use Time::localtime;
use File::Spec;
use File::Path qw(make_path);
use FindBin qw($Bin);
use Getopt::Long;
use File::Basename;
use Cwd qw(abs_path);
use Time::Piece;
use Sys::Hostname;

#add the directory containing the logger module to @INC
use lib "$FindBin::Bin/commons";
use Logger qw(log_message set_logfile set_criticalinfofile generate_run_id);
use LockFile;

#Script version and global variables 
my $VERSION = "1.0.0";
my $TODAY = localtime->strftime("%Y%m%d");
my $SCRIPT_DIR = $Bin;
my $DATA_DIR = File::Spec->catdir($SCRIPT_DIR, '..', 'data');
my $LOGDIR = File::Spec->catdir($SCRIPT_DIR, 'log');

# Debug logging
print "SCRIPT_DIR: $SCRIPT_DIR\n";
print "DATA_DIR: $DATA_DIR\n";
print "LOGDIR: $LOGDIR\n";

my $LOCKFILE = File::Spec->catfile($SCRIPT_DIR, "D31_Check_TempFiles_SpoolDirectory.lock");
my $is_windows = $^O eq 'MSWin32';
my $lock;

# If run_id is not provided, generate it dynamically
my ($run_id);

# Parse the command-line arguments using Getopt::Long
GetOptions(
    'run_id=s' => \$run_id  # Capture the run_id argument as a string
);

# Check if the run_id was provided and print it
if (defined $run_id) {
    print "Captured run_id: $run_id\n";
} else {
    $run_id = generate_run_id();
    print "Not received 'run_id' from Wrapper. Hence generating run_id dynamically. Run id is: $run_id\n";
}

my $LOGFILE = File::Spec->catfile($LOGDIR, "D31_Check_TempFiles_SpoolDirectory-${run_id}.log");
my $CRITICALINFOFILE = File::Spec->catfile($LOGDIR, "SOP_Runner_${run_id}.criticalinfo");

# Create the log directory if it does not exist
unless (-d $LOGDIR) {
    make_path($LOGDIR, { chmod => 0777 }) or do {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "Could not create log directory: $LOGDIR\n");
        exit 62;
    };
}

# Ensure the log file exists
unless (-e $LOGFILE) {
    open my $log_fh, '>', $LOGFILE or do {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "Could not create log file: $LOGFILE\n");
        exit 63;
    };
    close $log_fh;
}

unless (-e $CRITICALINFOFILE) {
    open my $critical_fh, '>', $CRITICALINFOFILE or do {
        log_message(Script_Type => 'CRITICALINFO', Data => "Could not create log file: $CRITICALINFOFILE\n");
        exit 63;
    };
    close $critical_fh;
}

# Set log files in Logger module
set_logfile($LOGFILE);
set_criticalinfofile($CRITICALINFOFILE);

# Acquire a lock to prevent multiple script instances from running simultaneously
sub acquire_lock {
    my $lock = LockFile->new($LOCKFILE, \&log_message);
    unless ($lock->acquire()) {
        log_message(Script_Type => 'CRITICALINFO', Data => "Could not acquire lock. Another instance of the script may be running.");
        exit 69;
    }
    return $lock;
}

# Main execution block
$lock = acquire_lock();

log_message(Script_Type => 'DIAGNOSTICS', Data => "Acquiring the lock before starting the script");
log_message(Script_Type => 'DIAGNOSTICS', Data => "Starting D31 script, version $VERSION");
log_message(Script_Type => 'DIAGNOSTICS', Data => "Running on host: " . hostname());
log_message(Script_Type => 'DIAGNOSTICS', Data => "Script is being run as part of validation step after APPD Alert");

# Determine the OS
my $os = $^O;
if ($os eq "MSWin32") {
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Operating System: Windows");
    
    # Spool directory path (default)
    my $spool_dir = "C:\\Windows\\System32\\spool\\PRINTERS";
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Spool Directory Path: $spool_dir");
    
    # Check spool directory
    opendir(my $dir, $spool_dir) or do {
        log_message(Script_Type => 'CRITICALINFO', Data => "Cannot open directory: $!");
        $lock->release();
        exit 1;
    };
    
    my @files = grep { -f "$spool_dir\\$_" } readdir($dir);
    closedir($dir);
    
    my $file_count = scalar @files;
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Number of Temp files in the Spool Directory: $file_count Files");
    
    if ($file_count > 0) {
        my @file_dates;
        foreach my $file (@files) {
            my $file_path = "$spool_dir\\$file";
            my $stat = stat($file_path);
            if ($stat) {
                push @file_dates, $stat->mtime;
                # Add debug logging
                log_message(Script_Type => 'DIAGNOSTICS', Data => "File: $file, mtime: " . ctime($stat->mtime));
            } else {
                log_message(Script_Type => 'CRITICALINFO', Data => "Unable to stat file: $file_path");
            }
        }
        
        if (@file_dates) {
            @file_dates = sort { $a <=> $b } @file_dates;  # Numeric sort
            my $earliest = localtime($file_dates[0])->strftime('%Y-%m-%d %H:%M:%S');
            my $latest = localtime($file_dates[-1])->strftime('%Y-%m-%d %H:%M:%S');
            
            # Add direct print statements
            print "Earliest file date: $earliest\n";
            print "Latest file date: $latest\n";
            
            log_message(Script_Type => 'DIAGNOSTICS', Data => "The earliest date for any file: $earliest");
            log_message(Script_Type => 'DIAGNOSTICS', Data => "The latest date for any file: $latest");
        } else {
            log_message(Script_Type => 'DIAGNOSTICS', Data => "No valid file dates found.");
        }
    } else {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "No files found in the Spool directory.");
    }

    # Get the list of printers
    my $printers_list = `powershell.exe -Command "Get-Printer | Select-Object -ExpandProperty Name"`;
    chomp($printers_list);
    my @printers = split("\n", $printers_list);
    
    # Check print jobs for each printer
    foreach my $printer (@printers) {
        # Clean up printer name for PowerShell
        my $escaped_printer = $printer;
        
        # Remove any leading \\ and server name if present
        $escaped_printer =~ s/^\\\\[^\\]+\\//;
        
        # Remove any printer share portion
        $escaped_printer =~ s/\\.*$//;
        
        # Escape any special characters
        $escaped_printer =~ s/(["\\\$`])/`$1/g;
        
        # Modified PowerShell command with error handling
        my $ps_command = qq{
            try {
                \$printer = Get-Printer -Name "$escaped_printer" -ErrorAction Stop
                \$jobs = Get-PrintJob -PrinterName \$printer.Name -ErrorAction Stop
                \$jobs.Count
            } catch {
                "0"
            }
        };
        
        my $print_jobs = `powershell.exe -Command "$ps_command"`;
        chomp($print_jobs);
        
        if ($print_jobs =~ /^\d+$/) {
            log_message(Script_Type => 'DIAGNOSTICS', 
                       Data => "Number of jobs in printer queue '$printer': $print_jobs");
        } else {
            log_message(Script_Type => 'DIAGNOSTICS', 
                       Data => "No active print jobs found for printer '$printer' or printer is not accessible");
        }
    }

} elsif ($os eq "linux") {
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Operating System: Linux");
    
    # Spool directory path (default)
    my $spool_dir = "/var/spool/lpd";
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Spool Directory Path: $spool_dir");
    
    # Check spool directory
    opendir(my $dir, $spool_dir) or do {
        log_message(Script_Type => 'CRITICALINFO', Data => "Cannot open directory: $!");
        $lock->release();
        exit 1;
    };
    
    my @files = grep { -f "$spool_dir/$_" } readdir($dir);
    closedir($dir);
    
    my $file_count = scalar @files;
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Number of Temp files in the Spool Directory: $file_count Files");
    
    if ($file_count > 0) {
        my @file_dates;
        foreach my $file (@files) {
            my $file_path = "$spool_dir/$file";
            my $stat = stat($file_path);
            if ($stat) {
                push @file_dates, $stat->mtime;
            } else {
                log_message(Script_Type => 'CRITICALINFO', Data => "Unable to stat file: $file_path");
            }
        }
        if (@file_dates) {
            @file_dates = sort @file_dates;
            my $earliest = ctime($file_dates[0]);
            my $latest   = ctime($file_dates[-1]);
            log_message(Script_Type => 'DIAGNOSTICS', Data => "The earliest date for any file: $earliest");
            log_message(Script_Type => 'DIAGNOSTICS', Data => "The latest date for any file: $latest");
        } else {
            log_message(Script_Type => 'DIAGNOSTICS', Data => "No valid file dates found.");
        }
    } else {
        log_message(Script_Type => 'DIAGNOSTICS', Data => "No files found in the Spool directory.");
    }

    # Get the list of printers more safely
    my $printers_list = `lpstat -p 2>/dev/null || echo ""`;
    chomp($printers_list);
    
    # Add debug logging
    log_message(Script_Type => 'DIAGNOSTICS', Data => "Raw printer list output:\n$printers_list");
    
    my @printers;
    
    # Modified regex to be more specific
    while ($printers_list =~ /^printer\s+(\S+?)\s+is/gm) {
        push @printers, $1;
    }
    
    # Debug log the found printers
    log_message(Script_Type => 'DIAGNOSTICS', 
               Data => "Found printers: " . join(", ", @printers));
    
    if (@printers) {
        # Check print jobs for each printer
        foreach my $printer (@printers) {
            # Skip if printer name is literally "printer"
            next if $printer eq "printer";
            
            my $print_jobs = `lpstat -o $printer 2>/dev/null | wc -l`;
            chomp($print_jobs);
            
            if ($? == 0) {
                log_message(Script_Type => 'DIAGNOSTICS', 
                           Data => "Number of jobs in printer queue '$printer': $print_jobs");
            } else {
                log_message(Script_Type => 'DIAGNOSTICS', 
                           Data => "Could not get print jobs for printer '$printer'");
            }
        }
    } else {
        log_message(Script_Type => 'DIAGNOSTICS', 
                   Data => "No printers found or lpstat command not available");
    }

} else {
    log_message(Script_Type => 'CRITICALINFO', Data => "Unsupported Operating System: $os");
    $lock->release();
    exit 1;
}

# Release the lock when done
$lock->release();
exit 0;
