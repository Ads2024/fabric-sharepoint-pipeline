import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from generate_sharepoint_links import generate_single_employee_link

class TestGenerateLinks(unittest.TestCase):
    def setUp(self):
        self.mock_access_token = "fake_token"
        self.mock_drive_id = "fake_drive_id"
        self.mock_headers = {"Authorization": "Bearer fake_token"}

    @patch('requests.get')
    def test_generate_link_file_not_found(self, mock_get):
        # Setup mock for file not found
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        employee = {'EmployeeID': '1234', 'Name': 'John Doe'}
        result, success = generate_single_employee_link(
            employee, self.mock_access_token, self.mock_drive_id, "Base", self.mock_headers
        )

        self.assertFalse(success)
        self.assertEqual(result['status'], 'Failed')
        self.assertEqual(result['sharepoint_link'], 'File Not Found')

    @patch('requests.post')
    @patch('requests.get')
    def test_generate_link_success(self, mock_get, mock_post):
        # Setup mock for file found
        file_response = MagicMock()
        file_response.status_code = 200
        file_response.json.return_value = {'id': 'file_123'}
        mock_get.return_value = file_response

        # Setup mock for link creation
        link_response = MagicMock()
        link_response.status_code = 200
        link_response.json.return_value = {'link': {'webUrl': 'https://sharepoint.com/link'}}
        mock_post.return_value = link_response

        employee = {'ID': '1234', 'Name': 'John Doe'}
        result, success = generate_single_employee_link(
            employee, self.mock_access_token, self.mock_drive_id, "Base", self.mock_headers
        )

        self.assertTrue(success)
        self.assertEqual(result['sharepoint_link'], 'https://sharepoint.com/link')

if __name__ == '__main__':
    unittest.main()
