#!/usr/bin/perl -w

use strict;
use Test::Simple tests => 2;
use lib qw/./;
use DBIx::TextSearch;

# Test only with postgres as I haven't built the rest yet.
use DBIx::TextSearch::Pg;


print "Testing DBIx::TextSearch::Pg\n";
print "============================\n\n";


print "database server: [localhost]";
my $server = <STDIN>;
chomp $server;
print "database port: [5432]";
my $port = <STDIN>;
chomp $port;
print "\ndatabase name: ";
my $db = <STDIN>;
chomp $db;
print "\ndatabase username: [$ENV{USER}]";


my $dbh = DBI->connect("dbi:pg:dbname=$db;host";
ok (defined $dbh);
