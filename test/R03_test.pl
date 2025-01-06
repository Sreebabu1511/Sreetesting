#!/usr/bin/perl
use strict;
use warnings;
use File::Find;
use File::Spec;
use File::Basename;
use Cwd 'abs_path';
use POSIX qw(strftime);
use File::Path qw(make_path);
use Sys::Hostname;
use File::stat;
use FindBin qw($Bin);
use Encode qw(encode decode);
use Time::Local;
use Getopt::Long;
use File::Find::Rule;
use Time::HiRes qw(time);  # For more precise timing
# Add the directory containing the logger module to @INC
use lib "$FindBin::Bin/commons";
use Logger qw(log_message set_logfile set_criticalinfofile generate_run_id);
use LockFile;
# Script version and global variables
my $VERSION = "1.0.0";
my $TODAY = strftime "%Y%m%d", localtime;
my $SCRIPT_DIR = $Bin;
my $DATA_DIR = File::Spec->catdir($SCRIPT_DIR, '..', 'data');
my $LOGDIR = File::Spec->catdir($DATA_DIR, '..', 'log');
my $LOCKFILE = File::Spec->catfile($SCRIPT_DIR, "R03_Highdiskusage.lock");
my $is_windows = $^O eq 'MSWin32';
my $lock;
# Other global variables
my $TOP_N = 20;
my @large_files;
my $file_count = 0;
my $last_update = time;
my $files_processed = 0;
# Set Windows console to use appropriate codepage
if ($is_windows) {
  binmode(STDOUT, ':raw');
}
# Parse run_id from command line
my ($run_id);
GetOptions(
  'run_id=s' => \$run_id
);
if (defined $run_id) {
  print "Captured run_id: $run_id\n";
} else {
  $run_id = generate_run_id();
  print "Not received 'run_id' from Wrapper. Hence generating run_id dynamically. Run id is: $run_id\n";
}
# Directory and file setup
unless (-d $LOGDIR) {
   make_path($LOGDIR, { chmod => 0777 }) or die "Could not create log directory: $LOGDIR\n";
}
my $LOGFILE = File::Spec->catfile($LOGDIR, "R03_Highdiskusage-${run_id}.log");
my $CRITICALFILE = File::Spec->catfile($LOGDIR, "SOP_Runner-CriticalInfo-${run_id}.criticalfile");
# Set up logging first
set_logfile($LOGFILE);
set_criticalinfofile($CRITICALFILE);
unless (-e $LOGFILE) {
   open my $log_fh, '>', $LOGFILE or do {
       log_message(Script_Type => 'DIAGNOSTICS', Data => "Could not create log file: $LOGFILE\n");
       exit 63;
   };
   close $log_fh;
}
unless (-e $CRITICALFILE) {
   open my $critical_fh, '>', $CRITICALFILE or do {
       log_message(Script_Type => 'CRITICALINFO', Data => "Could not create log file: $CRITICALFILE\n");
       exit 63;
   };
   close $critical_fh;
}
# Lock file handling
sub acquire_lock {
  my $lock = LockFile->new($LOCKFILE, \&log_message);
  unless ($lock->acquire()) {
      log_message(Script_Type => 'DIAGNOSTICS', Data => "Could not acquire lock. Another instance of the script may be running.");
      exit 69;
  }
  return $lock;
}
# Configuration constants
my $MIN_AGE_DAYS = 30;
my @BACKUP_PATTERNS = qw(BKP BKUP BACKUP BACK_UP);
my @INSTALL_BACKUP_PREFIXES = qw(DSC_ DSS_ MCS_ LES LM);
my @DUMP_EXTENSIONS = qw(.dmp .mdmp .hprof);
my @HOTFIX_EXTENSIONS = qw(.tar .tgz .gz);
# Add size threshold constant near other configuration constants
my $SIZE_THRESHOLD = 500 * 1024 * 1024;  # 500MB in bytes
# Pre-compile regular expressions (add after configuration constants)
my @BACKUP_PATTERNS_REGEX = map { qr/$_/i } @BACKUP_PATTERNS;
my @INSTALL_PREFIX_REGEX = map { qr/^$_/i } @INSTALL_BACKUP_PREFIXES;
my @DUMP_EXTENSIONS_REGEX = map { qr/$_$/i } @DUMP_EXTENSIONS;
my @HOTFIX_EXTENSIONS_REGEX = map { qr/$_$/i } @HOTFIX_EXTENSIONS;
# Get available drives once and cache the result
{
   my @cached_drives;
   sub get_available_drives {
       # Return cached drives if already detected
       return @cached_drives if @cached_drives;
       if ($is_windows) {
           for my $letter ('C'..'Z') {
               push @cached_drives, "$letter:\\" if -d "$letter:\\";
           }
       } else {
           # Modified Linux directory handling
           @cached_drives = (
               '/home',
               '/data',
               '/var',
               '/opt',
               '/usr',
               '/root'
           );
           # Only include directories that exist
           @cached_drives = grep { -d $_ } @cached_drives;
           
           # Add custom directories if they exist
           push @cached_drives, '/logs' if -d '/logs';
           push @cached_drives, '/rollouts' if -d '/rollouts';
       }
       return @cached_drives;
   }
}
# Function to handle directory traversal errors
sub handle_dir_error {
   my ($dir, $error) = @_;
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Cannot access directory: $dir - $error" );
   return;
}
# Function to let user select drive
sub select_drive {
   my @drives = get_available_drives();
   print "\nAvailable drives:\n";
   for my $i (0..$#drives) {
       print "[$i] $drives[$i]\n";
   }
   print "\nPlease select a drive number: ";
   my $selection = <STDIN>;
   chomp($selection);
   while ($selection !~ /^\d+$/ || $selection > $#drives) {
       print "Invalid selection. Please enter a number between 0 and $#drives: ";
       $selection = <STDIN>;
       chomp($selection);
   }
   print "\nYou selected: $drives[$selection]\n";
   return $drives[$selection];
}
# File processing function
sub wanted {
   my $file = $File::Find::name;
   my $dir = $File::Find::dir;
   eval {
       # Skip if path doesn't exist or isn't readable
       return unless -e $_ && -r $_;
       # Skip system directories and special paths
       return if $is_windows && $dir =~ /(?:system volume information|\$recycle\.bin|\$WINDOWS\.~BT)/i;
       return if !$is_windows && $dir =~ m{^(?:/proc|/sys|/dev|/run)};
       # Skip if symlink (optional, remove if you want to follow symlinks)
       return if -l $_;
       my $size = -s $_;
       return unless defined $size;  # Skip if can't get size
       my $stat = stat($_);
       return unless $stat;  # Skip if can't get stats
       # Check if file/folder is at least 30 days old
       my $age_days = (time - $stat->mtime) / (24 * 60 * 60);
       return unless $age_days >= $MIN_AGE_DAYS;
       my $should_flag = 0;
       my $reason = "";
       # Check backup patterns
       foreach my $pattern (@BACKUP_PATTERNS) {
           if ($file =~ /$pattern/i) {
               $should_flag = 1;
               $reason = "Backup file pattern match: $pattern";
               last;
           }
       }
       # Check installation backup prefixes
       if (!$should_flag) {
           foreach my $prefix (@INSTALL_BACKUP_PREFIXES) {
               if (basename($file) =~ /^$prefix/i) {
                   $should_flag = 1;
                   $reason = "Installation backup prefix match: $prefix";
                   last;
               }
           }
       }
       # Check dump files in specific directories
       if (!$should_flag && $dir =~ m{(?:MOCA/bin|/les)$}i) {
           foreach my $ext (@DUMP_EXTENSIONS) {
               if ($file =~ /$ext$/i) {
                   $should_flag = 1;
                   $reason = "Dump file in monitored directory";
                   last;
               }
           }
       }
       # Check hotfix files
       if (!$should_flag && $dir =~ m{/les/hotfix}i) {
           foreach my $ext (@HOTFIX_EXTENSIONS) {
               if ($file =~ /$ext$/i) {
                   $should_flag = 1;
                   $reason = "Hotfix archive file";
                   last;
               }
           }
       }
       if ($should_flag) {
           push @large_files, {
               path => $file,
               size => $size,
               reason => $reason
           };
       }
   };
   if ($@) {
       handle_dir_error($file, $@);
   }
}
# Update the scan_directory function
sub scan_directory {
   my ($start_point) = @_;
   my $files_processed = 0;
   my $matches_found = 0;
   my @large_files;
   
   if ($is_windows) {
       my $cmd = qq(dir /s /b /a:-d "$start_point" | findstr /v "^<" 2>NUL);
       my $pipe;
       open($pipe, '-|', $cmd) or die "Cannot execute command: $!";
       # ... process Windows files ...
       close($pipe) if defined $pipe;
   } else {
       my $cmd = qq(find "$start_point" -xdev \\( ! -path "*/proc/*" ! -path "*/sys/*" ! -path "*/dev/*" ! -path "*/run/*" \\) -type f -print0 2>/dev/null);
       
       my $pipe;
       if (open($pipe, '-|', "($cmd) | xargs -0 stat --format '%s %n' 2>/dev/null")) {
           while (my $line = <$pipe>) {
               chomp $line;
               my ($size, $file) = split(' ', $line, 2);
               next unless defined $size && defined $file;
               
               $files_processed++;
               
               # Only process files above threshold and older than MIN_AGE_DAYS
               next unless -f $file && $size >= $SIZE_THRESHOLD;
               my $stat = stat($file);
               next unless $stat;
               
               my $age_days = (time - $stat->mtime) / (24 * 60 * 60);
               next unless $age_days >= $MIN_AGE_DAYS;
               
               my ($should_flag, $reason) = check_file_patterns($file);
               if ($should_flag) {
                   $matches_found++;
                   push @large_files, {
                       path => $file,
                       size => $size,
                       reason => $reason
                   };
               }
           }
           close($pipe);
       } else {
           log_message(Script_Type => 'ERROR', Data => "Cannot execute command: $!");
           return $files_processed;
       }
   }

   # Only try to sort and display if we have files
   if (@large_files) {
       my $max_index = $#large_files < ($TOP_N-1) ? $#large_files : ($TOP_N-1);
       my @top_files = (sort { $b->{size} <=> $a->{size} } @large_files)[0..$max_index];
       
       log_message(Script_Type => 'CRITICALINFO', Data => sprintf("\nTop %d largest matching files:", scalar(@top_files)));
       foreach my $file (@top_files) {
           my $size_mb = sprintf("%.2f", $file->{size} / (1024 * 1024));
           log_message(
               Script_Type => 'CRITICALINFO',
               Data => sprintf("File: %s\nSize: %s MB\nReason: %s\n",
                   $file->{path},
                   $size_mb,
                   $file->{reason}
               )
           );
       }
   } else {
       log_message(Script_Type => 'CRITICALINFO', Data => "No matching files found during the scan.");
   }
   
   return $files_processed;
}
# New helper function to process batches of files
sub process_file_batch {
   my ($files, $large_files, $matches_ref) = @_;
   foreach my $file (@$files) {
       next unless -e $file && -f $file;
       my $stat = stat($file);
       next unless $stat;
       
       # Skip files smaller than threshold
       next if $stat->size < $SIZE_THRESHOLD;
       
       my ($should_flag, $reason) = check_file_patterns($file);
       if ($should_flag) {
           $$matches_ref++;
           push @$large_files, {
               path => $file,
               size => $stat->size,
               reason => $reason
           };
           # Keep only top N+100 files to save memory
           if (@$large_files > $TOP_N + 100) {
               @$large_files = (sort { $b->{size} <=> $a->{size} } @$large_files)[0..($TOP_N + 99)];
           }
       }
   }
}
# Helper function to check file patterns (same as before)
sub check_file_patterns {
   my ($file) = @_;
   # Check backup patterns
   for my $pattern (@BACKUP_PATTERNS_REGEX) {
       return (1, "Backup file pattern match") if $file =~ $pattern;
   }
   # Check installation backup prefixes
   my ($basename) = $file =~ m{([^/\\]+)$};
   if ($basename) {
       for my $prefix (@INSTALL_PREFIX_REGEX) {
           return (1, "Installation backup prefix match") if $basename =~ $prefix;
       }
   }
   # Check dump files
   if ($file =~ m{(?:moca/bin|/les)}i) {
       for my $ext (@DUMP_EXTENSIONS_REGEX) {
           return (1, "Dump file in monitored directory") if $file =~ $ext;
       }
   }
   # Check hotfix files
   if ($file =~ m{/les/hotfix}i) {
       for my $ext (@HOTFIX_EXTENSIONS_REGEX) {
           return (1, "Hotfix archive file") if $file =~ $ext;
       }
   }
   return (0, "");
}
# Update log_disk_usage to not log anything
sub log_disk_usage {
   # Empty function as we don't want to log disk usage
   return;
}
# Create required directories
sub create_required_directories {
   # First create data directory if it doesn't exist
   unless (-d $DATA_DIR) {
       eval {
           make_path($DATA_DIR, { chmod => 0777 });
           print "Created data directory: $DATA_DIR\n";
       };
       if ($@) {
           die "Could not create data directory: $DATA_DIR - $@\n";
       }
   }
   # Then create log directory if it doesn't exist
   unless (-d $LOGDIR) {
       eval {
           make_path($LOGDIR, { chmod => 0777 });
           print "Created log directory: $LOGDIR\n";
       };
       if ($@) {
           die "Could not create log directory: $LOGDIR - $@\n";
       }
   }
}
# Add directory creation before any logging operations
create_required_directories();
# Function to calculate folder size
sub get_folder_size {
   my ($folder) = @_;
   my $total_size = 0;
   eval {
       if ($is_windows) {
           # Windows-specific directory size calculation
           my $cmd = qq(dir /s /a "$folder" 2>NUL);
           open(my $pipe, '-|', $cmd) or die "Cannot execute dir command: $!";
           while (<$pipe>) {
               if (/File\(s\)\s+(\d+)\s+bytes/) {
                   $total_size += $1;
               }
           }
           close($pipe);
       } else {
           # Unix/Linux directory size calculation
           my $cmd = qq(du -sb "$folder" 2>/dev/null);
           open(my $pipe, '-|', $cmd) or die "Cannot execute du command: $!";
           if (<$pipe> =~ /^(\d+)/) {
               $total_size = $1;
           }
           close($pipe);
       }
   };
   if ($@) {
       log_message(Script_Type => 'WARNING', Data => "Error calculating size for folder $folder: $@");
   }
   return $total_size;
}
# Main Routine
eval {
   my $start_time = time();
   # Keep only the specified log messages
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Acquiring the lock before starting the script");
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Starting R03_Highdisk_usage, version $VERSION");
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Running on host: " . hostname());
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Script is being run as part of validation step after APPD Alert");
   my $selected_drive = select_drive();
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Available drives/mount points: " . join(", ", get_available_drives()));
   print "Starting analysis of $selected_drive. This may take several minutes for large drives...\n";
   log_message(Script_Type => 'DIAGNOSTICS', Data => "Starting scan of $selected_drive");
   my $total_files = scan_directory($selected_drive);
   # Calculate elapsed time and files per second (optional)
   my $elapsed_time = time() - $start_time;
   my $files_per_sec = $total_files / ($elapsed_time || 1);
   # Add the following print statements to display log file paths
   print "\nAnalysis completed.\n";
   print "Log file path: $LOGFILE\n";
   print "Critical info file path: $CRITICALFILE\n";
};
if ($@) {
   print "Error occurred: $@\n";
   log_message(Script_Type => 'ERROR', Data => "Script failed: $@");
   exit 1;
}
# Release lock before exit
$lock->release() if $lock;
exit 0;
