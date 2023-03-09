"""
This module handles User class
"""
from copy import deepcopy
from flask import session, has_request_context
from flask import g as application_context
from src.utils.config import Config
from src.utils.utils import clean_split


class User:
    """
    User class is responsible for handling user objects as well as providing
    convenience methods such as user name or authentication status
    Information is obtained from headers supplied by SSO proxy
    """

    def __init__(self):
        request_context = has_request_context()
        if request_context:
            self.user_info = self.__get_user_info(session_cookie=session)
            setattr(application_context, "user_info", self.user_info)
        else:
            if hasattr(application_context, "user_info"):
                self.user_info = application_context.user_info
            else:
                self.user_info = {
                    "username": "automatic",
                    "name": "automatic",
                    "email": "",
                    "authorized": False,
                }

    def __get_user_info(self, session_cookie):
        """
        Check request headers and parse user information
        """
        user_data = session_cookie.get("user")
        username = user_data.get("username")
        email = user_data.get("email")
        fullname = user_data.get("fullname")
        roles = set(user_data.get("roles"))
        user_info = {
            "username": username,
            "name": fullname,
            "email": email,
            "authorized": False,
        }

        authorized = set(clean_split(Config.get("authorized")))
        # Either group is authorized or a user
        user_info["authorized"] = len(roles & authorized) > 0 or username in authorized
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
        return self.user_info["username"]

    def get_name(self):
        """
        Get name and last name
        """
        return self.user_info["name"]

    def get_email(self):
        """
        Get user's email
        """
        return self.user_info["email"]

    def is_authorized(self):
        """
        Return whether user is authorized
        """
        return self.user_info["authorized"]
