
import mock

from django.core.management import call_command, CommandError
from xmodule.modulestore.tests.django_utils import ModuleStoreTestCase
from xmodule.modulestore.tests.factories import CourseFactory

from openedx.core.djangoapps.content.course_overviews.models import CourseOverview
from openedx.core.djangoapps.models.course_details import CourseDetails
from opaque_keys.edx.keys import CourseKey


class TestAddInstructorID(ModuleStoreTestCase):
    """
    Test add_instructor_ids management command.
    """

    def setUp(self):
        super(TestAddInstructorID, self).setUp()

        self.org = 'TextX'
        self.course = CourseFactory.create(
            org=self.org,
            instructor_info={
                'instructors': [
                    {
                        'name': 'test-instructor1',
                        'organization': 'TextX',
                    },
                    {
                        'name': 'test-instructor1',
                        'organization': 'TextX',
                    },

                ]
            }
        )

        self.course_key = unicode(self.course.id)

        # Creating CourseOverview Object from course descriptor because
        # we are filtering the courses by organizations in CourseOverview.
        self.course_overview = CourseOverview.load_from_module_store(self.course.id)

    def test_uuid_population_by_course_key(self):
        """
        Test population of instructor's uuid by course_keys.
        """

        call_command(
            "add_instructor_ids",
            "--username", self.user.username,
            "--course_keys", self.course_key
        )

        # Test uuids should be populated
        instructors = CourseDetails.fetch(CourseKey.from_string(self.course_key)).instructor_info
        for instructor in instructors.get("instructors", []):
            self.assertTrue("uuid" in instructor)

    def test_uuid_population_by_org(self):
        """
        Test population of instructor's uuid by organizations.
        """

        # Mocked the raw_input and returns 'n'
        with mock.patch('__builtin__.raw_input', return_value='n') as _raw_input:
            call_command(
                "add_instructor_ids",
                "--username", self.user.username,
                "--orgs", self.org
            )

        # Test uuids should not be populated
        instructors = CourseDetails.fetch(CourseKey.from_string(self.course_key)).instructor_info
        for instructor in instructors.get("instructors", []):
            self.assertFalse("uuid" in instructor)

        # Mocked the raw_input and returns 'y'
        with mock.patch('__builtin__.raw_input', return_value='y') as _raw_input:
            call_command(
                "add_instructor_ids",
                "--username", self.user.username,
                "--orgs", self.org
            )

        # Test uuids should be populated
        instructors = CourseDetails.fetch(CourseKey.from_string(self.course_key)).instructor_info
        for instructor in instructors.get("instructors", []):
            self.assertTrue("uuid" in instructor)

    def test_insufficient_args(self):
        """
        Test management command with insufficient arguments.
        """
        with self.assertRaises(CommandError):
            call_command(
                "add_instructor_ids",
                "--username", self.user.username,
            )
