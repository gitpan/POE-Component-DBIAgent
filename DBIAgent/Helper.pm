package POE::Component::DBIAgent::Helper;

=head1 NAME

POE::Component::DBIAgent::Helper - DBI Query Helper for DBIAgent

=head1 SYNOPSYS

 use Socket qw/:crlf/;
 use POE qw/Filter::Line Wheel::Run Component::DBIAgent::Helper/;

 sub _start {
     my $helper = POE::Wheel::Run ->new(
	     Program     => sub {
		 POE::Component::DBIAgent::Helper->run($self->{dsn},
						       $self->{queries}
						      );
	     },
	     StdoutEvent => 'db_reply',
	     StderrEvent => 'remote_stderr',
	     ErrorEvent  => 'error',
	     Filter => POE::Filter::Line->new(),
	     StdoutFilter => POE::Filter::Line->new( Literal => CRLF),
	    )
      or carp "Can't create new DBIAgent::Helper: $!\n";

 }

 sub query {
      my ($self, $query, $package, $state, @rest) = @_;

      $self->{helper}->put(join '|', $query, $package, $state, @rest);
 }

 sub db_reply {
    my ($kernel, $self, $heap, $input) = @_[KERNEL, OBJECT, HEAP, ARG0];

    # $input is either the string 'EOF' or a Storable object.

 }


=head1 DESCRIPTION

This is our helper routine for DBIAgent.  It accepts queries on STDIN,
and returns the results on STDOUT.  Queries are returned on a
row-by-row basis, followed by a row consisting of the string 'EOF'.

Each row is the return value of $sth->fetch, which is an arrayref.
This row is then passed to Storable for transport, and printed to
STDOUT.  HOWEVER, Storable uses newlines ("\n") in its serialized
strings, so the Helper is designed to use the "network newline" pair
CR LF as the line terminator for STDOUT.

When fetch() returns undef, one final row is returned to the calling
state: the string 'EOF'.  Sessions should test for this value FIRST
when being invoked with input from a query.

=head2 Initialization

The Helper has one public subroutine, called C<run()>, and is invoked
with two parameters:

=over

=item The DSN

An arrayref of parameters to pass to DBI->connect (usually a dsn,
username, and password).

=item The Queries.

A hashref of the form Query_Name => "$SQL".  See
L<POE::Component::DBIAgent> for details.

=back

=cut

use DBI;
#use Daemon; # qw//;
use Socket qw/:crlf/;
use Data::Dumper;
use Storable qw/freeze thaw/;

use strict;

use vars qw/$VERSION/;
$VERSION = sprintf("%d.%02d", q$Revision: 0.01 $ =~ /(\d+)\.(\d+)/);

#my $debug = 1;

use constant DEBUG => 0;
use constant DEBUG_UPDATE => 0;

sub run {

    DEBUG && warn "  QA: start\n";
    DEBUG_UPDATE && warn "  QA: NO UPDATE\n";

    my ($type, $dsn, $queries) = @_;

    my $self = bless {}, $type;
    $self->_init_dbi($dsn, $queries);

    $| = 1;

    # Input record seperator uses Network Newlines (so we know what to chomp);
    $/ = CRLF;

    $self->{dbh}->{RaiseError} = 0;
    $self->{dbh}->{PrintError} = 0;

    DEBUG && warn "  QA: initialized\n";

    my $row;			# to hold DBI result
    while ( <STDIN> ) {
	chomp;

	DEBUG && warn "  QA: Got line: $_\n";

	last if /^EXIT$/;	# allow parent to tell us to exit

	my( $id, $package, $state, @rest ) = split( /\|/, $_ );

	if ($id eq 'CREATE') {
	    next;
	}

	DEBUG && warn "  QA: Read line: $id for $state (params @rest)\n";
	#print STDOUT "Read line: $id", CRLF;

	unless (exists $self->{$id}) {
	    DEBUG && warn "  QA: No such query: $id";
	    #print "ERROR|No such query: $id", CRLF;
	    next;
	}
	DEBUG && warn "  QA: query $id exists\n";

	my $rowcount = 0;

	# This is true if $self->{$id} is a DBI statement handle.
	if (ref $self->{$id}) {
	    # Normal query loop.  This is where we usually go.
	    unless ( $self->{$id}->execute( @rest ) ) {
		DEBUG && warn "  QA: error executing query: ", $self->{$id}->errstr,"\n";

		#print "ERROR|", $self->{$id}->errstr, "\n";
	    } else {
		DEBUG && warn "  QA: query running\n";

		if ($self->{$id}{Active}) {
		    while (defined ($row = $self->{$id}->fetchrow_arrayref)) {

			$rowcount++;

			# Serialize via Storable: We use CRLF because Storable
			# uses \n in its streams, so we need to use \r\n for
			# end-of-line.
			print "$package|$state|", freeze($row), CRLF;
			#warn "  QA: got row $rowcount: ",,"\n";

		    }
		}
		print "$package|$state|EOF", CRLF;
		DEBUG && warn "  QA: ROWS|$rowcount\n";

	    }

	} else {		# *NOT* a DBI statement handle

	    # $queries->{$id} is a STRING of the query.  This is a debug
	    # feature.  Print a debug message, and send back EOF,
	    # but don't actually touch the database.
	    my $query = $queries->{$id};

	    # Replace ? placeholders with bind values.
	    $query =~ s/\?/shift @rest/eg;

	    DEBUG && warn "  QA: $query\n";
	    print "$package|$state|EOF", CRLF;

	}

    }

    $self->{dbh}->disconnect;

}


# {{{ _init_dbi

sub _init_dbi {
    my ($heap, $dsn, $queries) = @_;

    my $dbh = DBI->connect(@$dsn) or die DBI->errstr;
    $heap->{dbh} = $dbh;

    $dbh->{AutoCommit} = 1;
    $dbh->{RaiseError} = 0;

    #local $dbh->{RaiseError} = 1; # unless keys %hits; # There... it's FRESH

    if (defined $queries) {
	foreach (keys %$queries) {
	    if (DEBUG_UPDATE && $queries->{$_} =~ /insert|update|delete/i) {
		$heap->{$_} = $queries->{$_};
	    } else {
		$heap->{$_} = $dbh->prepare($queries->{$_}) or die $dbh->errstr;
	    }
	}

	return;
    }

}

# }}} _init_dbi

1;

__END__

=head1 BUGS

I have NO idea what to do about handling signals intelligently.
Specifically, under some circumstances, Oracle will refuse to
acknowledge SIGTERM (presumably since its libraries are non-reentrant)
so sometimes SIGKILL is required to terminate a Helper process.

=head1 AUTHOR

This module has been fine-tuned and packaged by Rob Bloodgood
E<lt>robb@empire2.comE<gt>.  However, most of the code came directly
from Fletch E<lt>fletch@phydeaux.orgE<gt>, either directly
(Po:Co:DBIAgent:Queue) or via his ideas.  Thank you, Fletch!

However, I own all of the bugs.

This module is free software; you may redistribute it and/or modify it
under the same terms as Perl itself.

=cut

