######################################################################
##                                                                  ##
##    TextSearch::Pg - Postgres specific routines for TextSearch    ##
##                                                                  ##
######################################################################

package DBIx::TextSearch;

######################################################################
sub CreateIndex {
    # create database tables
    # tables: _doc_ID - URI, title, docID
    #         _words  - w_ID, word
    #         _description   - m_ID, meta desription

    my $self = shift();
    my $dbh = $self->{dbh};

    my $index_name = $self->{name};

    # prep SQL statements to create index tables
    my $sql_docID = "create table $self->{name}_docID (" .
	"URI varchar(255), title varchar(100), d_ID int4, mtime bigint)";
    my $sql_words = "create table $self->{name}_words (" .
	"w_ID int4, word text)"; 
    my $sql_meta = "create table $self->{name}_description (" .
	"m_ID int4, description text)";

    # create index tables
    $dbh->do($sql_docID) or Carp::croak $dbh->errstr;
    $dbh->do($sql_words) or Carp::croak $dbh->errstr;
    $dbh->do($sql_meta)  or Carp::croak $dbh->errstr;
}
######################################################################
sub IndexFile {
    # given URI, title, description, document
    # contents, index this document calling syntax:
    # $self->IndexFile($params{uri}, $title, $description, $mtime  $content);
    my ($self, $uri, $title, $desc, $mtime, $content) = @_;

    my $doc = \$content; # HTML::TokeParser needs a ref to document
                         # content, DBI inserts the raw ref
                         # SCALAR(0x...) if passed a ref.

    # find a unique document ID number for this document
    my $sql_docid = "select d_id from " . $self->{name} . "_docID order " .
	"by d_id desc limit 1,0";
    my $sth_docid = $self->{dbh}->prepare($sql_docid);
    $sth_docid->execute();

    my $docid = $sth_docid->fetchrow_array();
    $sth_docid->finish();
    ++$docid;

    #
    # insert values into database.
    #

    # URI, title, doc_id, mtime
    my $sql_main = "insert into " . $self->{name} . "_docID " .
	"(URI, title, d_ID, mtime) values ('$uri', '$title', '$docid', '$mtime')";
    $self->{dbh}->do($sql_main) or say $sql_main;

    # words
    my $sql_words = "insert into $self->{name}_words " .
	"(w_ID, word) values ('$docid', '$content')";
    $self->{dbh}->do($sql_words);

    # meta description
    my $sql_meta = "insert into " . $self->{name} . "_description " .
	"(m_ID, description) values ('$docid', '$desc')";
    $self->{dbh}->do($sql_meta);

    # this document is now indexed.
}
######################################################################
sub GetQuery {
    # create and return a query via Text::Query::BuildSQLPg
    my $self = shift;
    my %params = @_;
    my $parser;

    # select either advanced parser or simple parser
    if ($params{parser} eq 'advanced') {
	$parser = new Text::Query($params{query},
				  -parse => 'Text::Query::ParseAdvanced',
				  -solve => 'Text::Query::SolveSQL',
				  -build => 'Text::Query::BuildSQLPg',
				  -fields_searched =>
				  'title, description, word'
				 );
    } elsif ($params{parser} eq 'simple') {
	$parser = new Text::Query($params{query},
				  -parse => 'Text::Query::ParseSimple',
				  -solve => 'Text::Query::SolveSQL',
				  -build => 'Text::Query::BuildSQLPg',
				  -fields_searched =>
				  'title, description, word'
				 );
    } else {
	Carp::croak "parser type not defined\n";
    }

    # generate the query
    my $query = "select distinct uri, title, description from " .
	"$self->{'name'}_docid, $self->{'name'}_description," .
	" $self->{'name'}_words where " . $parser->matchstring() .
	" and (( m_id=d_id) and (d_id=w_id))";
    return $query;
}
######################################################################
sub FlushIndex {
    # delete data from the index (not tables)
    my $self = shift();
    my @tables = ("$self->{name}_docid",
		  "$self->{name}_words",
		  "$self->{name}_description");

    foreach my $table (@tables) {
	$self->{dbh}->do("delete from $table") 
	    or Carp::cluck "Can't remove contents of index table $table: ". 
	      $dbh->errstr;
    }
}
######################################################################
sub MTime {
    # return the mtime for an index document
    # timestamp of indexed file
    my ($self, $doc) = @_;
    my $qry = "select mtime from $self->{name}_docID where " .
      "uri = '$doc'";
    say "query for indexed timestamp: $qry\n";
    my $sth = $self->{dbh}->prepare($qry);
    $sth->execute;
    my @time = $sth->fetchrow_array;
    my $dbtime = $time[0];
    my $rows = $sth->rows;
    # set db mtime to -1 if document not already indexed to make sure
    # that documents not already index get indexed.
    if ($rows == 0) { $dbtime = '-1' }
    return $dbtime;
}
######################################################################
sub RemoveDocument {
    # remove a single document from the database
    my ($self, $doc) = @_;
    my $sql = "delete from $self->{name}_docid, $self->{name}_words, " .
      "$self->{name}_description where uri='$loc' and ((w_id = d_id) and ".
      "(m_id = d_id))";
    $self->{dbh}->do($sql);
}
######################################################################
1;
