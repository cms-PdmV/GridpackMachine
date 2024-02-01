"""
This module handles User class
"""
from copy import deepcopy
from flask import request
from flask import g as request_context
from environment import AUTHORIZED
from src.tools.utils import clean_split


class User():
    """
    User class is responsible for handling user objects as well as providing
    convenience methods such as user name or authentication status
    Information is obtained from headers supplied by SSO proxy
    """

    def __init__(self):
        if not request_context:
            # Not in a request context
            self.user_info = {'username': 'automatic',
                              'name': 'automatic',
                              'email': '',
                              'authorized': False,}
        else:
            if hasattr(request_context, 'user_info'):
                self.user_info = request_context.user_info
            else:
                self.user_info = self.__get_user_info(request.headers)
                setattr(request_context, 'user_info', self.user_info)

    def __get_user_info(self, headers):
        """
        Check request headers and parse user information
        """
        username = headers.get('Adfs-Login')
        email = headers.get('Adfs-Email')
        fullname = headers.get('Adfs-Fullname')
        groups_header: str = headers.get('Adfs-Group', '')
        groups_header = groups_header.replace(',', ';')
        groups = set(x.strip().lower() for x in groups_header.split(';'))
        user_info = {'username': username,
                     'name': fullname,
                     'email': email,
                     'authorized': False}

        authorized = set(clean_split(AUTHORIZED))
        # Either group is authorized or a user
        user_info['authorized'] = len(groups & authorized) > 0 or username in authorized
        return user_info

    def get_user_info(self):
        """
        Return a copy of user info
        """
        return deepcopy(self.user_info)

    def get_username(self):
        """
        Get username, i.e. login name
        """
        return self.user_info['username']

    def get_name(self):
        """
        Get name and last name
        """
        return self.user_info['name']

    def get_email(self):
        """
        Get user's email
        """
        return self.user_info['email']

    def is_authorized(self):
        """
        Return whether user is authorized
        """
        return self.user_info['authorized']
