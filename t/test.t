#!/usr/bin/perl

use strict;
use Test::Simple tests => 3;
use DBI;
use DBIx::TextSearch;
use DBIx::TextSearch::Pg;
use vars qw/%ENV/;

# set env vars DB_SERVER, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_NAME.
# as per DBD::Pg docs also recognises PGOPTIONS and PGTTY

my $db_server   = 'localhost';
my $db_port     = '5432';
my $db_username = $ENV{USER};
my $db_password = undef();
my $db_name     = 'template1';

if (defined($ENV{DB_SERVER}   )) { $db_server   = $ENV{DB_SERVER}   }
if (defined($ENV{DB_PORT}     )) { $db_port     = $ENV{DB_PORT}     }
if (defined($ENV{DB_USERNAME} )) { $db_username = $ENV{DB_USERNAME} }
if (defined($ENV{DB_PASSWORD} )) { $db_password = $ENV{DB_PASSWORD} }
if (defined($ENV{DB_NAME}     )) { $db_name     = $ENV{DB_NAME}     }


# first off, open a database connection
my $dbh = DBI->connect("dbi:Pg:dbname=$db_name;host=$db_server;port=$db_port",
		       $db_username, $db_password);
ok(defined $dbh, 'connected to database server');

# create an index
#my $index = DBIx::TextSearch->new($dbh,
#				  'test_index');
#ok($index);

# disconnect from and reconnect to the index
#undef($index);

my $index = DBIx::TextSearch->open($dbh,
				'test_index');
ok($index);

# index a document - this module's page on cpan
my $rtn = $index->index_document('http://search.cpan.org/author/SRPATT/DBIx-TextSearch-0.1/lib/DBIx/TextSearch.pm');
ok($rtn);
exit;
undef($rtn);

# and search it
$rtn = $index->find_document(query => 'Patterson',
			     parser => 'simple');
ok($rtn);


# clean up
$index->flush_index();
$dbh->do('drop table test_index_doc_ID');
$dbh->do('drop table test_index_words');
$dbh->do('drop table test_index_description');
$dbh->disconnect();
