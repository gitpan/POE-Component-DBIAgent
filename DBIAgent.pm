package POE::Component::DBIAgent;

# {{{ POD

=head1 NAME

POE::Component::DBIAgent - POE Component for running asynchronous DBI calls.

=head1 SYNOPSIS

 sub _start {
    my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];

    $heap->{helper} = POE::Component::DBIAgent->new( DSN => [$dsn,
					       $username,
					       $password
					      ],
				       Queries => $self->make_queries,
				       Debug => 1,
				     );

	# Queries takes a hashref of the form:
	# { query_name => 'select blah from table where x = ?',
	#   other_query => 'select blah_blah from big_view',
	#   etc.
	# }

    $heap->{helper}->query(query_name => session => 'get_row_from_dbiagent');

 }

 sub get_row_from_qa {
    my ($kernel, $self, $heap, $row) = @_[KERNEL, OBJECT, HEAP, ARG0];
    if ($row ne 'EOF') {

 # {{{ PROCESS A ROW

	#row is a listref of columns

 # }}} PROCESS A ROW

    } else {

 # {{{ NO MORE ROWS

	#cleanup code here

 # }}} NO MORE ROWS

    }

 }


=head1 DESCRIPTION

The DBIAgent is your answer to non-blocking DBI in POE.

It fires off child processes (configurable, defaults to 3) and feeds
database queries to it via two-way pipe (or however Wheel::Run is able
to manage it).  The only method is C<query()>.

=head2 Usage

Not EVERY query should run through the DBI agent.  If you need to run
a short query within a state, sometimes it can be a hassle to have to
define a whole seperate state to receive its value.  The determining
factor, of course, is how long your query will take.  If you are
trying to retrieve one row from a properly indexed table, use
C<$dbh-E<gt>selectrow_array()>.  If there's a join involved, or
multiple rows, or a view, use DBIAgent.  If it's a longish query and
startup costs don't matter to you, do it inline.  If startup costs DO
matter, use the Agent.

=head2 Return Values

The state in the session specified in the call to C<query()> will
receive in its ARG0 parameter the return value from the query.  If
your query returns multiple rows, then your state will be called
multiple times, once per row.  ADDITIONALLY, your state will be called
one time with ARG0 containing the string 'EOF'.  For DML (INSERT,
UPDATE, DELETE), this is the only time.  A way to utilise this might
be as follows:

 sub some_state {
     #...
     if ($enough_values_to_begin_updating) {

	 $heap->{dbiagent}->query(update_values_query =>
				  this_session =>
				  update_next_value =>
				  shift @{$heap->{values_to_be_updated}}
				 );
     }
 }

 sub update_next_value {
     my ($self, $heap) = @_[OBJECT, HEAP];
     # we got 'EOF' in ARG0 here but we don't care

     for (1..3) {		# Do three at a time!
	 my $value;
	 last unless defined ($value = shift @{$heap->{values_to_be_updated}});
	 $heap->{dbiagent}->query(update_values =>
				  this_session =>
				  update_next_value =>
				  $value
				 );
     }

 }

=cut

# }}} POD

use Socket qw/:crlf/;
#use Data::Dumper;
use Storable qw/freeze thaw/;
use Carp;

use strict;
use POE qw/Session Filter::Line Wheel::Run Component::DBIAgent::Helper Component::DBIAgent::Queue/;

use vars qw/$VERSION/;

$VERSION = sprintf("%d.%02d", q$Revision: 0.12 $ =~ /(\d+)\.(\d+)/);

use constant DEFAULT_KIDS => 3;

#sub debug { $_[0]->{debug} }
sub debug { 1 }
#sub debug { 0 }

#sub carp { warn @_ }
#sub croak { die @_ }

# {{{ new

=head2 new()

Creating an instance creates a POE::Session to manage communication
with the Helper processes.  Queue management is transparent and
automatic.  The constructor is named C<new()> (surprised, eh?  Yeah,
me too).  The parameters are as follows:

=over

=item DSN

An arrayref of parameters to pass to DBI->connect (usually a dsn,
username, and password).

=item Queries

A hashref of the form Query_Name => "$SQL".  For example:

 {
   sysdate => "select sysdate from dual",
   employee_record => "select * from emp where id = ?",
   increase_inventory => "update inventory
                          set count = count + ?
                          where item_id = ?",
 }

As the example indicates, DBI placeholders are supported, as are DML statements.

=item Count

The number of helper processes to spawn.  Defaults to 3.  The optimal
value for this parameter will depend on several factors, such as: how
many different queries you'll be running, how much RAM you have, how
often you run queries, and how many queries you intend to run
I<simultaneously>.

=back

=cut

sub new {
    my $type = shift;

    croak "$type needs an even number of parameters" if @_ & 1;
    my %params = @_;

    my $dsn = delete $params{DSN};
    croak "$type needs a DSN parameter" unless defined $dsn;
    croak "DSN needs to be an array reference" unless ref $dsn eq 'ARRAY';

    my $queries = delete $params{Queries};
    croak "$type needs a Queries parameter" unless defined $queries;
    croak "Queries needs to be a hash reference" unless ref $queries eq 'HASH';

    my $count = delete $params{Count} || DEFAULT_KIDS;
    #croak "$type needs a Count parameter" unless defined $queries;

    # croak "Queries needs to be a hash reference" unless ref $queries eq 'HASH';

    my $debug = delete $params{Debug} || 0;
    if ($debug) {
	# $count = 1;
    }

    # Make sure the user didn't pass in parameters we're not aware of.
    if (scalar keys %params) {
	carp( "unknown parameters in $type constructor call: ",
	      join(', ', sort keys %params)
	    );
    }
    my $self = bless {}, $type;
    my $config = shift;

    $self->{dsn} = $dsn;
    $self->{queries} = $queries;
    $self->{count} = $count;
    $self->{debug} = $debug;

    POE::Session->new( $self,
		       [ qw [ _start _stop db_reply remote_stderr error ] ]
		     );
    return $self;

}

# }}} new

# {{{ query

=head2 query(I<query_name>, I<session>, I<state>, [ I<parameter, parameter, ...> ])

The C<query()> method takes at least three parameters, plus any bind
values for the specific query you are executing.

=over

=item Query Name

This parameter must be one of the keys to the Queries hashref you
passed to the constructor.  It is used to indicate which query you
wish to execute.

=item Session, State

These parameters indicate the POE state that is to receive the data
returned from the database.  The state indicated will receive the data
in its C<ARG0> parameter.  I<PLEASE> make sure this is a valid state,
otherwise you will spend a LOT of time banging your head against the
wall wondering where your query data is.

=item Query Parameters

These are any parameters your query requires.  B<WARNING:> You must
supply exactly as many parameters as your query has placeholders!
This means that if your query has NO placeholders, then you should
pass NO extra parameters to C<query()>.

Since this is the first release of this module, suggestions to improve
this syntax are welcome.  Consider this subject to change.

=back

=cut

sub query {
    my ($self, $query, $package, $state, @rest) = @_;

    $self->debug && carp "QA: Running query ($query) for package ($package) to state ($state)\n";

    $self->{helper}->next->put(join '|', $query, $package, $state, @rest);

}

# }}} query

# {{{ STATES

# {{{ _start

sub _start {
    my ($self, $kernel, $heap, $dsn, $queries) = @_[OBJECT, KERNEL, HEAP, ARG0, ARG1];

    $self->debug && carp __PACKAGE__ . " received _start.\n";

    # make this session accessible to the others.
    #$kernel->alias_set( 'qa' );

    my $queue = POE::Component::DBIAgent::Queue->new();

    ## Input and output from the children will be line oriented
    foreach (1..$self->{count}) {
	my $helper = POE::Wheel::Run->new(
					  Program     => sub {
					      POE::Component::DBIAgent::Helper->run($self->{dsn}, $self->{queries});
					  },
					  StdoutEvent => 'db_reply',
					  StderrEvent => 'remote_stderr',
					  ErrorEvent  => 'error',
					  StdinFilter => POE::Filter::Line->new(),
					  StderrFilter => POE::Filter::Line->new(),
					  StdoutFilter => POE::Filter::Line->new( Literal => CRLF),
					 )
	  or carp "Can't create new Wheel::Run: $!\n";
	$self->debug && carp __PACKAGE__, " Started db helper pid ", $helper->PID, " wheel ", $helper->ID, "\n";
	$queue->add($helper);
    }

    $self->{helper} = $queue;

}

# }}} _start
# {{{ _stop

sub _stop {
    my ($self, $heap) = @_[OBJECT, HEAP];

    $self->{helper}->kill_all();

    # Oracle clients don't like to TERMinate sometimes.
    $self->{helper}->kill_all(9);

    $self->debug && carp __PACKAGE__ . " has stopped.\n";

}

# }}} _stop

# {{{ db_reply

sub db_reply {
    my ($kernel, $self, $heap, $input) = @_[KERNEL, OBJECT, HEAP, ARG0];

    # Parse the "receiving state" and dispatch the input line to that state.

    my ($package, $state, $rest) = split /\|/, $input, 3;
    my $obj;

    # $self->debug && $self->debug && carp "QA: received db_reply for $package => $state\n";

    unless (defined $rest) {
	$self->debug && carp "QA: Empty input value.\n";
	return;
    }

    if ($rest eq 'EOF') {
	$self->debug && warn "QA: Got EOF\n";
	$obj = $rest;
    } else {

	eval { $obj = thaw($rest) };

	unless (defined $obj) {
	    if ($@) {
		warn "QA: Data error: $@\n";
	    } else {
		$self->debug && carp "QA: Undefined error... corrupt input line?\n"
	    }
	}
    }

    if (defined $obj) {
	if (0 && $self->debug) {
	    if ($obj eq 'EOF') {
		carp "Calling $package => $state => 'EOF'\n";
	    } else {
		carp "Calling $package => $state => \$obj\n";
		#carp Data::Dumper->Dump([$obj],[qw/$obj/]);
	    }
	}
	$kernel->call($package => $state => $obj);
    }


}

# }}} db_reply

# {{{ remote_stderr

sub remote_stderr {
    my ($self, $operation, $errnum, $errstr, $wheel_id) = @_[OBJECT, ARG0..ARG3];

    #$self->debug && carp "read from qa: $operation";

    my $error = $operation;

    $self->debug && carp "$operation: $errstr\n";

}

# }}} remote_stderr
# {{{ error

sub error {
    my ($self, $operation, $errnum, $errstr, $wheel_id) = @_[OBJECT, ARG0..ARG3];

    $self->debug && carp "error: Wheel $wheel_id generated $operation error $errnum: $errstr\n";

}

# }}} error

# }}} STATES

1;

__END__

=head1 NOTES

=over

=item *

Error handling is practically non-existent.

=item *

The calling syntax is still pretty weak.

=item *

I might eventually want to support returning hashrefs, if there is any
demand.

=item *

Every query is prepared at Helper startup.  This could potentially be
pretty expensive.  Perhaps a cached or deferred loading might be
better?  This is considering that not every helper is going to run
every query, especially if you have a lot of miscellaneous queries.

=back

Suggestions welcome!  Diffs *more* welcome! :-)

=head1 AUTHOR

This module has been fine-tuned and packaged by Rob Bloodgood
E<lt>robb@empire2.comE<gt>.  However, alot the code came directly from
Fletch E<lt>fletch@phydeaux.orgE<gt>, either directly
(Po:Co:DBIAgent:Queue) or via his ideas.  Thank you, Fletch!

However, I own all of the bugs.

This module is free software; you may redistribute it and/or modify it
under the same terms as Perl itself.

=cut
