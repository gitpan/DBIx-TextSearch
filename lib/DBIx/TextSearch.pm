######################################################################
##                                                                  ##
##    TextSearch - generic module to index and search ASCII files   ##
##    This will hook up to a set of database specific routines      ##
##                                                                  ##
######################################################################


package DBIx::TextSearch;

use DBI;
use Carp qw(croak cluck); # croak is die, cluck is warn;
use Env qw(TEMP TMP);
use English;
use Net::FTP;
use LWP::Simple;
use LWP::UserAgent;
use HTTP::Response;
use SGML::StripParser;
use Text::Query;
use URI;
use File::Basename;
use HTML::TokeParser;
use strict;
use warnings;
use Socket;
use Sys::Hostname;
$VERSION = '0.1';

######################################################################
sub say {
    print @_;
}
######################################################################
#sub AUTOLOAD {
#    # allow variables to be accessed directly via methods
#    # i.e. print $index->match() or $index->match = 5;
#    my $self = shift;
#    my $type = ref $self or Carp::croak "$self is not an object";
#    my $field = $AUTOLOAD;
#    $field =~ s/.*://s;
#    unless (exists $self->{$field}) {
#	Carp::croak "$field does not exist in object/class $type";
#    }
#    if (@_) {
#	return $self->($name) = shift;
#    } else {
#	return $self->($name);
#    }
#}
######################################################################
sub new {
    # create an index
    # takes as input: database handle, index name
    my $type = shift;
    my $self = {};
    my $dbh = shift;
    my $name = shift;

    # set database handle
    $self->{dbh} = $dbh;

    # set index name
    $self->{name} = $name;

    # test for sufficient parameters
    unless ($self->{dbh}) {
	croak "A database handle is required.\n";
    }
    unless ($self->{name}) {
	croak "An index name is required.\n";
    }

    bless $self, $type;
    # create appropriate tables
    $self->CreateIndex or croak "Can't create index\n";

    # end.
    return $self, $type;
}
######################################################################
sub open {
    # connect to an existing index (or barf if there isn't one)
    my $type = shift;
    my $self = {};
    my $dbh = shift;
    my $name = shift;

    # set database handle
    $self->{dbh} = $dbh;

    # set index name
    $self->{name} = $name;

    # test for sufficient parameters
    unless ($self->{dbh}) {
	Carp::croak "A database handle is required.\n";
    }
    unless ($self->{name}) {
	Carp::croak "An index name is required.\n";
    }

    # check if this index exists
    my @tables = $dbh->tables;
    my $exists = undef;
    foreach my $table (@tables) {
	if ($table =~ m/$name/) {
	    # index tables exist
	    $exists = "True";
	}
    }

    # die with message if index tables don't exist
    if ($exists ne "True") {
	Carp::croak "Index $name is not available";
    }

    # else return an index object
    return bless $self, $type;
}
######################################################################
sub _get_unique_filename {
    # get a unique local filename to store a remote file as
    my ($i, $temp_dir);
    $i = 0;

    if ($ENV{TEMP}) {
	$temp_dir = $ENV{TEMP};
    } elsif ($ENV{TMP}) {
	$temp_dir = $ENV{TMP};
    } else {
	$temp_dir = '/tmp';
    }
    while (-e "$temp_dir/$PID.$i") {
	++$i;
    }
    return "$temp_dir/$PID.$i";
}
######################################################################
sub _ftp {
    say "fetching via ftp\n";
    # fetch a file via ftp and store locally
    my $url = shift();
    # parse an url like ftp://user:password@foo.bar.com/wibble/barf.txt into
    # something usable
    my $uri = URI->new($url);
    my $host = $uri->host();
    my $path = $uri->path();
    my $auth = $uri->authority(); # user:password@host

    my ($username, $passwd);
    if ($auth =~ /:/) {
        # get username and password from $auth
	$username = $auth;
	$username =~ s/:.*//;
	$passwd = $auth;
	$passwd =~ s/$username://;
	$passwd =~ s/@.*//;
	say "auth'd ftp\nusername: $username\nPassword $passwd\n"
    } else {
	# set username to anonymous, password to local (linux) email address
	$username = 'anonymous';
	my $hostname = `hostname`; # need to get domain name as well.
	my $me = $ENV{USER};
	$passwd = $me . '@' . $hostname;
	say "anon ftp\nuser : $username\npass : $passwd\n";
    }

    # remove remote file name from $path into a separate variable
    my $dir = dirname($path);
    my $remote_file = basename($path);


    # get unique name for local file
    my $local_file = _get_unique_filename();

    # fetch the file
    say "logging into $host as $username with password $passwd\n";
    my $ftp = Net::FTP->new($host,
			   Debug => 1,
			   Passive => 1);
    $ftp->login($username, $passwd);
    $ftp->cwd("$dir");
    $ftp->ascii();
    $ftp->get($remote_file, $local_file);
    $ftp->quit();

    # file transferred, return its location
    say "Local file is: $local_file\n";
    return $local_file;
}
######################################################################
sub _http {
    # fetch a file via http and store locally
    my $url = shift;


    # get unique name for local file
    my $local_file = _get_unique_filename();

    # fetch the file
    my $ua = LWP::UserAgent->new;
    my $request = HTTP::Request->new('GET', $url);
    my $response = $ua->request($request);

    if ($response->is_success) {
	# sucessful fetch
	# write to disk
	my $html = $response->content;
	CORE::open(HTML, ">$local_file") or
	  Carp::croak "Can't save HTML file $url to $local_file: $!";
	print HTML $html;
	close HTML;
	# file transferred, return its location
	return $local_file;
    } else {
	# error message
	my $error = $response->status_line;
	cluck $error;
    }

}
######################################################################
sub _rem_newer {
    # check the mtime of a URI against db. Return 1 if db file is older
    # else return 0
    # par 1 = http|ftp|file. par2 = uri
    my $self = shift();
    my ($ftype, $loc) = @_;
    my $mtime; # file change time
    my $dbtime = MTime($self, $loc); # mtime of indexed file

    say "is file newer than already indexed version?\n";
    if ($ftype eq 'http') {
	say "checking age with http\n";
	my @http = head($loc);
	$mtime = $http[2];
    } elsif ($ftype eq 'file') {
	my @file = stat($loc);
	$mtime = $file[9];
    } elsif ($ftype eq 'ftp') {
	    my $uri = URI->new($loc);
	    my $host = $uri->host();
	    my $path = $uri->path();
	    my $auth = $uri->authority(); # user:password@host
	    my ($username, $passwd);
	    if ($auth =~ /:/) {
		# get username and password from $auth
		say "authorized ftp\n";
		$username = $auth;
		$username =~ s/:.*//;
		$passwd = $auth;
		$passwd =~ s/$username://;
		$passwd =~ s/@.*//;
		say "Username: $username\nPassword: $passwd\n"
	    } else {
		# set username to anonymous, password to local (linux)
		# email address
		say "anonymous ftp\n";
		$username = 'anonymous';
		my $localhost = hostname();
		my $me = $ENV{USER};
		$passwd = $me . '@' . $localhost;
		say "Username: $username\nPassword: $passwd\n"
	    }

	    say "connecting to check timestamp of $path on server $host\n";
	    my $ftp = Net::FTP->new($host, Debug => 1) or die $@;
	    say "logging in with username $username password $passwd\n";
	    $ftp->login($username, $passwd);
	    say "checking timestamp of $path\n";
	    $mtime = $ftp->mdtm($path);
    }

    say "file timestamp : $mtime\nindex timestamp: $dbtime\n";

    # mtimes are intervals since the epoch.
    if ($mtime > $dbtime) {
	# remote file is newer than one stored in database
	say "uri is newer than indexed version\n";
	return ($mtime, '1');
    } else {
	$mtime = 0;
	say "uri is not newer than indexed version\n";
	return ($mtime, '0');
    }

}
######################################################################
sub index_document {
    # given a document URI, add it to the index.
    # each word is to be indexed once.
    # also, only index if file is newer than database copy.
    my ($file, $http_content_type, @head, $toIndex, $mtime, $newer, $toRemove);
    my $self = shift();
    my %params = @_;
    $toRemove = 0; # define these vars to anything
    $toIndex = 0;  # they're only checked later for eq 'Yes'
    $http_content_type = 0;

    my $uri = $params{uri};

    say "about to index $uri\n";

    # get file contents

    # if an ftp or http uri, call a sub to fetch the remote file, save
    # it somewhere useful (/tmp) and return the name that the file has
    # been saved under ($file)
    my $url = URI->new($uri) or say "couldn't create URI object to check url options\n";
    say "url is $uri\n";
    say "URI object is $url\n";

    if ($url->scheme() eq 'ftp') {
	# an FTP address
	# fetch and index only if remote file is newer than db
	($mtime, $newer) = _rem_newer($self, 'ftp', $uri);
	if ($mtime == '-1') { $toRemove = 'Yes'}
	if ($newer == 1) {
	    $file = _ftp($uri);
	    $toIndex = 'Yes';
	} else { $toIndex = 0 }
    } elsif ($url->scheme() eq 'http') {
	# an HTTP address
	# fetch and index only if remote file is newer than db
	say "fetching $uri via http\n";
	($mtime, $newer) = _rem_newer($self, 'http', $uri);
	if ($mtime == '-1') {
	    $toRemove = 'Yes';
	    say "need to remove olf version of this doc from index\n";
	}
	if ($newer == 1) {
	    $file = _http($uri);
	    @head = head($uri);
	    $http_content_type = shift(@head);
	    $toIndex = 'Yes';
	} else { $toIndex = 0 }
    } elsif ($url->scheme() eq 'file') {
	# a local file
	my $orig_file = $url->path();
	($mtime, $newer) = _rem_newer($self, 'file', $orig_file);
	if ($mtime == '-1') { $toRemove = 'Yes'}
	if ($newer == 1) {
	   $file = _get_unique_filename;
	   system('cp', $orig_file, $file);
	   $toIndex = 'Yes';
       } else { $toIndex = 0 }
    } else {
	Carp::cluck "Unrecognised URI type, assuming $uri is a local file\n";
	($mtime, $newer) = _rem_newer($self, 'file', $uri);
	if ($mtime == '-1') { $toRemove = 'Yes'}
	if ($newer == 1) {
	    $file = _get_unique_filename;
	    system('cp', $uri, $file);
	    $toIndex = 'Yes';
	} else { $toIndex = 0 }
    }

    # OK, now know where the file is, can open and index it

    # index the file differently depending on whether HTML or not. Use
    # either file extension or HTTP content type
    # HTML files get meta
    # description tag as description, all others get 1st paragraph of
    # file
    if ($toRemove eq 'Yes') {
	# remove old document first
	say "removing old document from index\n";
	RemoveDocument($self, $uri);
    }
    if ($toIndex eq 'Yes') {
	if ( ($uri =~ /html$|htm$/i) or ($http_content_type =~ /html/) ) {
	    say "processing $uri as html\n";
	    &_store_html($self,
			 file  => $file,
			 uri   => $uri,
			 mtime => $mtime);
	} else {
	    &_store_plain($self,
			  file  => $file,
			  uri   => $uri,
			  mtime => $mtime);
	}
    }
}
######################################################################
sub _uniqify {
    # uniqify a string
    my $str = shift;
    my $prev = 0;

    my @words = split(/ /, $str);
    @words = sort @words;
    my @out = grep($_ ne $prev && ($prev = $_, 1), @words);
    my $uniq = join(' ', @out);
    return $uniq;
}
######################################################################
sub _store_plain {
    # index the content of a local copy of an ASCII file, giving it the
    # uri of wherever it came from initially

    # get parameters
    my $self = shift();
    my %params = @_;
    # open the file
    CORE::open (INFILE, "<$params{file}");
    die unless $params{mtime};
    # use the first non-blank line line as the title
    # and the next paragraph as the description
    my $RS = "\n\n"; # read data terminated by 2 newlines
    my @parags = <INFILE>;
    my $title = $parags[0];
    my $description = $parags[1];

    # reform @parags array into a string of words
    my $doc = join(" ", @parags);
    # tidy up doc so it contains only alnums and single spaces
    $doc =~ s/[^A-Za-z]+/ /gm;
    $doc = _uniqify($doc);
    $title =~ s/[^A-Za-z]+/ /gm;
    $description =~ s/[^A-Za-z]+/ /gm;

    # store this data
    $self->IndexFile($params{uri}, $title, $description, $params{mtime}, $doc);
    say "URI  : $params{uri}\n",
        "Title: $title\n",
	"Desc : $description\n",
	"Doc: $doc\n";
    # remove local copy of file
    unlink($params{file});
}
######################################################################
sub _store_html {
    # index the content of a local copy of an HTML file, giving it the
    # uri of wherever it came from initially.

    # get parameters
    my $self = shift();
    my %params = @_;
    my ($title, $description, $keywords);
    $title = $keywords = $description = 0;

    # title
    my $HTML_Parser = new HTML::TokeParser($params{file});
    if ($HTML_Parser->get_tag("title")) {
	$title = $HTML_Parser->get_trimmed_text;
    } else {
	$title = 'Untitled Document';
    }
    # description and keywords
    while (my $token = $HTML_Parser->get_tag('meta')) {
	if ($token->[1]{name} eq 'description') {
	    # description
	    $description = $token->[1]{content};
	    $description =~ s/\n/ /g;
	} elsif ($token->[1]{name} eq 'keywords') {
	    # keywords
	    $keywords = $token->[1]{content};
	    $keywords =~ s/\n/ /g;
	}
    }

    # now have the title, description and keywords seperated out, can
    # dump the rest of the HTML code
    my $plain = _get_unique_filename;
    CORE::open PLAIN, ">$plain" or die $!;
    CORE::open INFILE, $params{file};
    my $sgmlp = new SGML::StripParser;
    $sgmlp->set_outhandle(\*PLAIN); # plain text output file
    $sgmlp->parse_data(\*INFILE);   # html input file
    close INFILE;
    close PLAIN;

    # read the non-html file in.
    undef $RS;
    CORE::open PLAIN, $plain or die $!;
    my $doc = <PLAIN>;
    # tidy up doc so it contains only alnums and single spaces
    $doc =~ s/[^A-Za-z]+/ /gm;
    close PLAIN;
    # uniqify $doc
    $doc = _uniqify($doc);
    $doc .= $keywords; # add keywords to doc content

    # store this data
    $self->IndexFile($params{uri}, $title, $description, $params{mtime}, $doc);

    # clean up temp files
    unlink($plain);
    unlink($params{file});
}
######################################################################
sub find_document {
    # read an altavista style advanced query in, pass through to an appropriate
    # Text::Query::BuildSQL interface and return the URI, title and
    # description of each matching document
    my $self = shift;
    my %params = @_;
    die unless ($self->{dbh});
    # get a fully parsed query to run
    $params{query} = $self->GetQuery(query  => $params{query},
				     parser => $params{parser});
    say "Got SQL query\n";
    die unless $params{query};
    say "Query: $params{query}\n";
    # the column names which will be returned when this query is run
    # are uri, title, description (in that order)

    say  "preparing query\n";
    my $sth = $self->{dbh}->prepare($params{query}) or
      Carp::cluck "error preparing query: $self->{dbh}->errstr";
    say "executing query (", time(), ")\n";
    $sth->execute or
      cluck "Can't execute query $params{query}: $self->{dbh}->errstr";

    # build an array of hashrefs - each hashref refers to a single row
    # by column name
    say "Building result array of hashrefs\n";
    my (@documents, $hash_row);
    while ($hash_row = $sth->fetchrow_hashref) {
	die $self->{dbh}->errstr() unless $hash_row;
	push @documents, $hash_row;
    	$self->{matches} = scalar(@documents);
    }
    # die unless  @documents;
    return \@documents;
}
#####################################################################
sub DESTROY {
    my $self = shift();
    $self->{dbh}->disconnect;
}
######################################################################
sub flush_index {
    my $self = shift();
    $self->FlushIndex();
}
######################################################################
sub delete_document {
    my ($self, $uri) = @_;
    $self->RemoveDocumnt($uri);
}
######################################################################
__END__

=pod

=head1 NAME

DBIx::TextSearch

=head1 SYNOPSIS

Database independent modules to index and search text/HTML
files. Supports indexing local files and fetching files by HTTP and FTP.

 use DBIx::TextSearch;
 use DBIx::TextSearch::Pg; # to use postgresql

 $dbh = DBI->connect(...); # see the DBD documentation

 $index = DBIx::TextSearch->new($dbh,
                               'index_name');

 $index = DBIx::TextSearch->open($dbh,
                                 ' index_name');

 # uri is file:/// ftp:// or http://
 $index->index_document(uri => $location);

 # $results is a ref to an array of hashrefs
 $results = $index->find_document(query => 'foo and not bar',
                                  parser => 'simple');
 $results = $index->find_document(query => 'foo and not bar',
                                  parser => 'advanced');

 foreach my $doc (@$results) {
     print "Title: ", $doc->{title}, "\n";
     print "Description: ", $doc->{description}, "\n";
     print "Location: ", $doc->{uri}, "\n";
 }

 $index->delete_document('http://localhost/foo.txt');

 $index->flush_index(); # clear the index

=head1 DESCRIPTION

DBIx::TextSearch consists of an abstraction layer (TextSearch.pm)
providing a set of standard routines to index text and HTML files.
These routines interface to a set of database specific routines (not
separately documented) in much the same way as the perl DBI and
DBD::foo modules do.

=head1 METHODS

=head2 new

 $index = DBIx::TextSearch->new($dbh,
                                'index_name');

Create a new index on the database referenced by $dbh. The database
must exist.

=head2 open

 $index = DBIx::TextSearch->open($dbh,
                                 'index_name');

Connect to an existing index

=head2 index_document

Given a file:/// http:// or ftp:// URI, fetch and index the document.

For each document, this method stores the document URI, the document
title, a document description, keywords (HTML only from <meta name="keywords"),
the document contents and the document's modification time. If the URI
points to a html file, the document title is
taken from the contents of the HTML <title> tag and the description
is taken from the contents of <meta name="description">. The HTML tags
are removed before finally storing the document. If the URI
is plain text (i.e. not HTML), the title is the first non-blank line
and the description is the next paragraph (terminated by 2 newlines)

index_document compares the file's modification time against the
modification time for that URI stored in the index, and will only
index a document if that document is not already in the index, or if
the document is more recent than the indexed copy.

For file:/// URIs, you need to include the full (absolute) path.

=head3 FTP passwords

Pass the username and password in the ftp URI as shown here:
C<ftp://user:password@foo.bar.com/wibble/barf.txt>

=head3 Sample URIs

 file://usr/doc/HOWTO/en-html/index.html
 /usr/doc/HOWTO/en-html/index.html
 http://www.foo.bar.com/
 ftp://foo.bar.com/wibble/barf.txt # anonymous - uses local email
	 			   # address as password
 ftp://user:password@foo.bar.com/wibble/barf.txt

=head2 find_document

This method takes 2 parameters: 

=head3 query

A boolean query string as per Text::Query::ParseSimple or
Text::Query::ParseAdvanced (an AltaVista style query)

=head3 parser

Either C<simple> or C<advanced> to use either Text::Query::ParseSimple or
Text::Query::ParseAdvanced to parse the query.

find_document returns a reference to an array of
hash references. The hash keys are URI, title, description.

The number of documents found by the last query is returned by
C<$index->match()>

To print information on all the documents matching a query, see this
code:

 my $results = find_document("zot or grault");

 foreach my $doc (@$results) {
     print "Title: ", $doc->{title}, "\n";
     print "Description: ", $doc->{description}, "\n";
     print "Location: ", $doc->{uri}, "\n";
 }

 print $index->matches(), " results found";

=head2 flush_index

Remove all stored document data from the index, leaving the index
tables intact.

=head2 delete_document

Given a URI, remove that document from the index.

=head1 SEE ALSO

Text::Query::ParseAdvanced, Text::Query::ParseSimple  DBI

=head1 AUTHOR

Stephen Patterson <s.patterson@freeuk.com> http://www.lexx.uklinux.net/

=cut
