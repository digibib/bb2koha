#!/usr/bin/perl 

# Copyright 2014 Oslo Public Library

=head1 NAME

bb2koha.pl - Download data from Base Bibliotek and load them into Koha.

=head1 SYNOPSIS

Use or download today's file:

 perl bb2koha.pl -c config.yml -m mapping.yml -v -d

Use or download the file for a given day:

 perl bb2koha.pl -c config.yml -m mapping.yml --date 2015-02-06

Use a file that has already been downloaded:

 perl bb2koha.pl -c config.yml -m mapping.yml --file /path/to/file.xml

=head1 PREREQUISITES

This script assumes the addition to the Koha REST API provided on this bug is
in place:

  Bug 13607 - Patron management API
  L<http://bugs.koha-community.org/bugzilla3/show_bug.cgi?id=13607>

=head1 FILES FROM BaseBibliotek (BB)

Files are published here: L<http://www.nb.no/baser/bibliotek/eksport/biblev/>
at 9 pm Monday-Friday. You need a username/password to access the files. 

This script assumes that it will normally be run from cron some time after 9 pm
and before midnight, so that the default file to use will be the one from the 
same date the script is run. 

File naming conventions:

Daily diffs are called bb-YYYY-MM-DD.xml

Complete dumps are called bb-full.xml

If there are no changes on a given day, a file is not present for that day 
(so we need to be prepared for 404 Not Found.)

=cut

use Catmandu::Importer::XML;
use YAML::Syck;
use LWP::UserAgent;
use XML::Simple;
use String::Util qw(trim);
use Getopt::Long;
use Data::Dumper;
use DateTime;
use Pod::Usage;
use Modern::Perl;

my $ua = LWP::UserAgent->new();
$ua->cookie_jar({});

# Get options
my ( $configfile, $mapfile, $date, $full, $file, $limit, $verbose, $debug ) = get_options();

=head1 CONFIGURATION FILES

=head2 Main config file

See config.yml-sample

=cut

# Check that the config file exists, and read it
if ( !-e $configfile ) {
    die "The config file $configfile does not exist!\n";
}
my $config = LoadFile( $configfile );
say "Read $configfile" if $verbose;
# Set the username and password for BB
$ua->credentials( 'www.nb.no:80', 'Eksport fra Base Bibliotek', $config->{'bbuser'}, $config->{'bbpass'} );

=head2 Metadata mapping file

See mapping.yml-sample

=cut

# Check that the mapping file exists, and read it
if ( !-e $mapfile ) {
    die "The map file $mapfile does not exist!\n";
}
my $map = LoadFile( $mapfile );
say "Read $mapfile" if $verbose;

# Determine the BB file to read
my $bbfile = '';
if ( $full ) {
    # We want the full file
    $bbfile = $config->{'datadir'} . 'bb-full.xml';
    say "Going to use $bbfile" if $verbose;
    get_file_from_bb({
        'urlfrag' => 'full',
        'bbfile'  => $bbfile
    });
} else {
    # If a specific date was given we use that, otherwise the default is today
    my $urlfrag = '';
    if ( $date eq '' ) {
        my $dt = DateTime->now;
        $urlfrag = $dt->ymd;
    } else {
        $urlfrag = $date;
    }
    $bbfile = $config->{'datadir'} . 'bb-' . $urlfrag . '.xml';
    say "Going to use $bbfile" if $verbose;
    if (!-e $bbfile) {
        # Try to download it from BB (will die if it fails)
        get_file_from_bb({
            'urlfrag' => $urlfrag,
            'bbfile'  => $bbfile
        });
    }
}

# Read the BB file
my $importer = Catmandu::Importer::XML->new(
    'file' => $bbfile,
    'path' => '/BaseBibliotek/record', # XML path
);

# Authenticate with BB
my $auth_resp = make_request({
    'diag'    => 'Authenticate',
    'urlfrag' => 'authentication',
    'data'    => { 
        'userid'   => $config->{'userid'}, 
        'password' => $config->{'password'},
    },
});
if ( $auth_resp->{'success'} == 0 ) {
    die 'Authentication failed, unable to proceed: ' . $auth_resp->{'msg'};
}

# Loop over the records (libraries)
my $records_count = 0;
if ( $limit eq '' ) {
    $limit = $importer->count;
}
$importer->take( $limit )->each( sub {

    my $record = shift;
    $record = $record->{'record'};

    # Put the data in a hashref as dictated by the mapping file, so we can 
    # pass it directly to the API later    
    my $librarydata = {};
    foreach my $key ( keys %{ $map } ) {
        $librarydata->{ $key } = $record->{ $map->{ $key } };
    }
    $librarydata->{ 'matchField' }   = $config->{ 'matchfield' };
    $librarydata->{ 'branchcode' }   = $config->{ 'branchcode' };
    $librarydata->{ 'categorycode' } = $config->{ 'categorycode' };
    say Dumper $librarydata if $debug;
    
    my $resp = make_request({
        'diag'    => 'Upsert',
        'urlfrag' => 'members/upsert',
        'data'    => $librarydata
    });
    if ( $resp->{'success'} == 0 || $verbose ) {
        say $resp->{'msg'};
    }

    $records_count++;

} );

if ( $verbose ) {
    say "$records_count of " . $importer->count . " records processed";
}

=head1 SUBROUTINES

=head2 get_file_from_bb

Downloads a file from Base Bibliotek and saves it in the location specified by
the B<datadir> config variable. 

If the desired file can not be downloaded, this subroutine will die.

Arguments:

=over 4

=item * B<urlfrag> - fragment for use in the URL, either a date in YYYY-MM-DD 
format, or the string 'full'

=item * B<bbfile> - full path to where the file should be saved

=cut

sub get_file_from_bb {

    my ( $args ) = @_;
    my $url = 'http://www.nb.no/baser/bibliotek/eksport/biblev/bb-' . $args->{'urlfrag'} . '.xml';
    say "Trying to download $url to " . $args->{'bbfile'} . '...' if $verbose;
    my $resp = $ua->mirror( $url, $args->{'bbfile'} );
    say $resp->status_line if $verbose;
    say "Done" if $verbose;
    if ( !-e $args->{'bbfile'} ) {
        # say Dumper $resp;
        die "Could not download $url";
    }

}

=head2 make_request

Make a request to the API, passing along any data and printing the result

Arguments:

=over 4

=item * B<diag> - a short description of what is being done, to aid in debugging.

=item * B<urlfrag> - the URL fragment that should be added onto the B<endpoint>
variable set in the config. Do not include a slash at the beginning, this will
be added autmatically.

=item * B<data> - a hashref containing patron/library data, that can be passed
directly to the API.

=back

Returns a hashref containing the following elements:

=over 4

=item * B<success> - a boolean (0/1) indicating if the request was successful or not

=item * B<response> - the respons from the API, as a hashref

=item * B<status> - the HTTP status code from the request

=item * B<msg> - a (more or less) human readable summary of the response from the
request, suitable for reporting to the user and/or for logging

=back

=cut

sub make_request {

    my ( $args ) = @_;
    
    say '*** ' . $args->{'diag'} . ' ***' if $debug;
    my $url = $config->{ 'endpoint' } . '/cgi-bin/koha/svc/' . $args->{'urlfrag'};
    say "Talking to $url" if $debug;
    my $resp = $ua->post( $url, $args->{'data'} );
    my $resp_data = XMLin( $resp->decoded_content );

    if ( $debug ) {
        say $resp->status_line;
        say $resp->decoded_content;
        say Dumper $resp_data;
    }
    
    my $success = 0;
    if ( $resp_data->{'status'} eq 'ok' ){
        $success = 1;
    }
    
    # Construct a readble message
    my $msg = '';
    if ( $args->{'data'}->{'cardnumber'} ) {
        $msg = $args->{'data'}->{'cardnumber'} . ': ' . $resp->status_line . ' - ';
        foreach my $key ( sort keys %{ $resp_data } ) {
            $msg .= $key . '="' . trim( $resp_data->{$key} ) . '" ';
        }
    } else {
        $msg = $resp->status_line . ' - ' . $resp_data->{'status'};
    }
    
    return {
        'success'  => $success,
        'response' => $resp_data,
        'status'   => $resp->status_line,
        'msg'      => $msg,
    }

}

=head1 OPTIONS

=over 4

=item B<-c, --configfile>

Use the given config file.

=item B<-m, --mapfile>

Use the given config file to map between fields in BB and Koha.

See L<http://schema.koha-community.org/tables/borrowers.html> for which fields 
are available in Koha.

=item B<-d, --date>

Specify a date to import data from. This will first look for the file 
corresponding to the given date in the directory specified in the "datadir"
config variable. If it is not found there the script will try to download it
from BB. If a file is found by any of these means, the data wil be imported into
Koha. 

Dates should be given in the format YYYY-MM-DD.

Do not combine this option with --file. 

=item B<--full>

Download and ingest F<bb-full.xml>. If the file can not be downloaded the script
will die. If you have already downloaded F<bb-full.xml> and want to use that,
do not use this option, but give the path to the file with the --file option.

=item B<-f, --file>

Import a specific BB file (that has already been downloaded). This can be
either a daily "diff" file or a full file.

Do not combine this option with --date. 

=item B<-l, --limit>

Only process the n first records.

=item B<-v --verbose>

More verbose output.

Without this option, only requests to the API that are not successfull will 
give any output. With it, all requests will give output, and the general progress
of the script will be reported on. 

=item B<--debug>

Even more verbose output. This will dump things like responses from the API. 

=item B<-h, -?, --help>

Prints this help message and exits.

=back
                                                               
=cut

sub get_options {

    # Options
    my $configfile = '';
    my $mapfile    = '';
    my $date       = '';
    my $full       = '';
    my $file       = '';
    my $limit      = '', 
    my $verbose    = '';
    my $debug      = '';
    my $help       = '';

    GetOptions (
        'c|configfile=s' => \$configfile,
        'm|mapfile=s'    => \$mapfile,
        'd|date=s'       => \$date,
        'full'           => \$full,   
        'f|file=s'       => \$file,
        'l|limit=i'      => \$limit,
        'v|verbose'      => \$verbose,
        'debug'          => \$debug,
        'h|?|help'       => \$help,
    );

    pod2usage( -exitval => 0 ) if $help;
    pod2usage( -msg => "\nMissing Argument: -c, --configfile required\n",      -exitval => 1 ) if !$configfile;
    pod2usage( -msg => "\nMissing Argument: -m, --mapfile required\n",         -exitval => 1 ) if !$mapfile;
    pod2usage( -msg => "\nSpecifying a date and a file does not make sense\n", -exitval => 1 ) if $file ne '' && $date ne '';
    pod2usage( -msg => "\nInvalid date format: $date\n",                       -exitval => 1 ) if $date ne '' && $date !~ /^20\d\d-[01]\d-[0123]\d$/;
    pod2usage( -msg => "\nThe file $file does not exist\n",                    -exitval => 1 ) if $file ne '' && !-e $file;

    return ( $configfile, $mapfile, $date, $full, $file, $limit, $verbose, $debug );

}

=head1 AUTHOR

Magnus Enger, Oslo Public Library

=head1 LICENSE

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
