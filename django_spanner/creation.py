# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import os
import sys
from unittest import skip

from django.conf import settings
from django.db.backends.base.creation import BaseDatabaseCreation
from django.utils.module_loading import import_string


class DatabaseCreation(BaseDatabaseCreation):
    """
    Spanner-specific wrapper for Django class encapsulating methods for
    creation and destruction of the underlying test database.
    """

    def mark_skips(self):
        """Skip tests that don't work on Spanner."""
        for test_name in self.connection.features.skip_tests:
            test_case_name, _, method_name = test_name.rpartition(".")
            test_app = test_name.split(".")[0]
            # Importing a test app that isn't installed raises RuntimeError.
            if test_app in settings.INSTALLED_APPS:
                test_case = import_string(test_case_name)
                method = getattr(test_case, method_name)
                setattr(
                    test_case,
                    method_name,
                    skip("unsupported by Spanner")(method),
                )

    def create_test_db(self, verbosity=1, autoclobber=False, serialize=True, keepdb=False):
        """Create a test database.

        :rtype: str
        :returns: The name of the newly created test Database.
        """
        # This environment variable is set by the Travis build script or
        # by a developer running the tests locally.
        if os.environ.get("RUNNING_SPANNER_BACKEND_TESTS") == "1":
            self.mark_skips()
        from django.core.management import call_command

        test_database_name = self._get_test_db_name()

        if verbosity >= 1:
            action = 'Creating'
            if keepdb:
                action = "Using existing"

            self.log('%s test database for alias %s...' % (
                action,
                self._get_database_display_str(verbosity, test_database_name),
            ))

        # We could skip this call if keepdb is True, but we instead
        # give it the keepdb param. This is to handle the case
        # where the test DB doesn't exist, in which case we need to
        # create it, then just not destroy it. If we instead skip
        # this, we will get an exception.
        self._create_test_db(verbosity, autoclobber, keepdb)
        self.connection.close()
        settings.DATABASES[self.connection.alias]["NAME"] = test_database_name
        self.connection.settings_dict["NAME"] = test_database_name
        # We then serialize the current state of the database into a string
        # and store it on the connection. This slightly horrific process is so people
        # who are testing on databases without transactions or who are using
        # a TransactionTestCase still get a clean database on every test run.
        if serialize:
            self.connection._test_serialized_contents = self.serialize_db_to_string()
        call_command('createcachetable', database=self.connection.alias)
        # Ensure a connection for the side effect of initializing the test database.
        self.connection.ensure_connection()
        return test_database_name

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Create dummy test tables. This method is mostly copied from the
        base class but removes usage of `_nodb_connection` since Spanner doesn't
        have or need one.
        """
        # Mostly copied from the base class but removes usage of
        # _nodb_connection since Spanner doesn't have or need one.
        test_database_name = self._get_test_db_name()
        # Don't quote the test database name because google.cloud.spanner_v1
        # does it.
        test_db_params = {"dbname": test_database_name}
        # Create the test database.
        try:
            self._execute_create_test_db(None, test_db_params, keepdb)
        except Exception as e:
            # If the db should be kept, then no need to do any of the below,
            # just return and skip it all.
            if keepdb:
                return test_database_name
            self.log("Got an error creating the test database: %s" % e)
            if not autoclobber:
                confirm = input(
                    "Type 'yes' if you would like to try deleting the test "
                    "database '%s', or 'no' to cancel: " % test_database_name
                )
            if autoclobber or confirm == "yes":
                try:
                    if verbosity >= 1:
                        self.log(
                            "Destroying old test database for alias %s..."
                            % (
                                self._get_database_display_str(
                                    verbosity, test_database_name
                                ),
                            )
                        )
                    self._destroy_test_db(test_database_name, verbosity)
                    self._execute_create_test_db(None, test_db_params, keepdb)
                except Exception as e:
                    self.log(
                        "Got an error recreating the test database: %s" % e
                    )
                    sys.exit(2)
            else:
                self.log("Tests cancelled.")
                sys.exit(1)
        return test_database_name

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        self.connection.instance.database(parameters["dbname"]).create()

    def _destroy_test_db(self, test_database_name, verbosity):
        self.connection.instance.database(test_database_name).drop()
