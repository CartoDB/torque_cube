all:
	@echo "Try make check"

check:
	psql -f pyramid.sql
	psql -XtA -v "VERBOSITY=terse" < cdb_torquepixel_add_test.sql 2>&1 | diff -U2 cdb_torquepixel_add_test_expected -

