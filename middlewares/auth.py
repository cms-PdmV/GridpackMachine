"""
This module implements an authentication middleware to
register a client to handle OAuth 2.0 authentication requests.
"""
import os
import re
import jwt
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
from authlib.integrations.flask_client import OAuth
from werkzeug.exceptions import HTTPException
from flask.sessions import SessionMixin
from flask import (
    Flask,
    Blueprint,
    Request,
    Response,
    session,
    redirect,
    url_for,
    jsonify,
)


class AuthenticationMiddleware:
    """
    This AuthenticationMiddleware sets OAuth 2.0 authentication for a Flask application
    It handles the authentication by JWT access token and it is able to refresh
    expired access tokens if a refresh token is available.
    :param app: Flask application
    :param client_id: OAuth 2.0 Application Client ID
    :param client_secret: OAuth 2.0 Application Client Secret
    :param valid_audiences: Authorized audiences (applications) whose tokens are
                            accepted by the web server
    """

    OIDC_CONFIG_DEFAULT: str = (
        "https://auth.cern.ch/auth/realms/cern/.well-known/openid-configuration"
    )
    JWT_PUBLIC_KEY_URL: str = (
        "https://auth.cern.ch/auth/realms/cern/protocol/openid-connect/certs"
    )
    JWT_REGEX_PATTERN: str = (
        r"eyJ([a-zA-Z0-9_=]+)\.([a-zA-Z0-9_=]+)\.([a-zA-Z0-9_\-\+\/=]*)"
    )

    def __init__(
        self,
        app: Flask,
        client_id: str,
        client_secret: str,
        home_endpoint: str,
        valid_audiences: list[str] = None,
    ):
        self.oidc_config: str = os.getenv(
            "REALM_OIDC_CONFIG", AuthenticationMiddleware.OIDC_CONFIG_DEFAULT
        )
        self.jwt_public_key_url: str = os.getenv(
            "REALM_PUBLIC_KEY_URL", AuthenticationMiddleware.JWT_PUBLIC_KEY_URL
        )
        self.jwt_regex_pattern: str = AuthenticationMiddleware.JWT_REGEX_PATTERN
        self.jwt_regex = re.compile(self.jwt_regex_pattern)
        self.app: Flask = self.__configure_session_cookie_security(app=app)
        self.home_endpoint: str = home_endpoint
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.valid_audiences: list[str] = (
            [self.client_id] if valid_audiences is None else valid_audiences
        )
        self.jwk: jwt.PyJWK = self.__retrieve_jwk()
        self.oauth_client: OAuth = self.__register_oauth_client()
        self.oauth_blueprint: Blueprint = self.__register_blueprint()

    def __auth(self):
        """
        This endpoint starts the communication with the OAuth 2.0 Authorization Server
        to request an access and refresh token.
        """
        redirect_uri: str = url_for("oauth.callback", _external=True)
        return self.oauth_client.cern.authorize_redirect(redirect_uri)

    def __callback(self):
        """
        This endpoint handles the callback from the OAuth 2.0 Authorization Server and
        stores the access and refresh tokens inside a cookie handled by the Flask.
        Also, this endpoint redirects the user back to its original destination.
        """
        try:
            token = self.oauth_client.cern.authorize_access_token()
            session["token"] = {
                "access_token": token["access_token"],
                "refresh_token": token["refresh_token"],
            }
            original_destination: str = session.pop(
                "next", default=url_for(self.home_endpoint)
            )
            return redirect(original_destination)
        except Exception:
            return redirect(url_for(self.home_endpoint))

    def __configure_session_cookie_security(self, app: Flask) -> Flask:
        """
        Restrict the access to the session cookie.
        The session cookie is going to be used to store the JWT token to authenticate the user,
        the user data decrypted and the next endpoint the user is going to be redirected after a successful authentication.
        Based on Flask documentation, the session cookie is cryptographically signed when it is transmitted to the
        client web browser. For more information, please see: https://flask.palletsprojects.com/en/2.2.x/quickstart/?highlight=session#sessions
        :return: Flask application with session cookie security set
        :rtype: Flask
        """
        # Configure the session cookie
        app.config["SESSION_COOKIE_SAMESITE"] = "None"
        app.config["SESSION_COOKIE_HTTPONLY"] = True
        app.config["SESSION_COOKIE_SECURE"] = True
        return app

    def __register_blueprint(self) -> Blueprint:
        """
        Register a submodule (blueprint) inside the Flask application to
        handle OAuth authentication. The new submodule is registered under the
        /oauth2 url prefix.
        :return Flask submodule (blueprint)
        :rtype Blueprint
        """
        oauth_blueprint = Blueprint("oauth", __name__)
        # Register views
        oauth_blueprint.add_url_rule(
            rule="/auth", endpoint="auth", view_func=self.__auth
        )
        oauth_blueprint.add_url_rule(
            rule="/callback", endpoint="callback", view_func=self.__callback
        )
        # Register OAuth submodule into the application
        self.app.register_blueprint(blueprint=oauth_blueprint, url_prefix="/oauth2")
        return oauth_blueprint

    def __register_oauth_client(self) -> OAuth:
        """
        Register the OAuth 2.0 Client into the Flask application used to build token claim
        requests.
        :return OAuth 2.0 Client
        :rtype OAuth
        """
        # Set the client id and secret
        client_credentials: dict = {
            "CERN_CLIENT_ID": self.client_id,
            "CERN_CLIENT_SECRET": self.client_secret,
        }

        # Update the application to include this environment variables
        self.app.config.from_mapping(client_credentials)

        # Register CERN Realm
        oauth_client: OAuth = OAuth(app=self.app)
        oauth_client.register(
            name="cern",
            server_metadata_url=self.oidc_config,
            client_kwargs={
                "scope": "openid profile email",
            },
        )
        return oauth_client

    def __retrieve_jwk(self) -> jwt.PyJWK:
        """
        Retrieve the public key from the OAuth 2.0 Authorization Server used to
        validate JWT access token.
        :return JWK to validate JWT access token
        :rtype PyJWK
        """
        jwks_client = jwt.PyJWKClient(self.jwt_public_key_url)
        return jwks_client.get_signing_keys()[0]

    def __token_to_user(self, decoded_token: dict) -> dict:
        """
        Parse the user data included inside the JWT access token
        and return the user information.
        :return CERN user information
        :rtype dict
        """
        username: str = decoded_token.get("sub")
        roles: list[str] = decoded_token.get("cern_roles")
        email: str = decoded_token.get("email")
        given_name: str = decoded_token.get("given_name")
        family_name: str = decoded_token.get("family_name")
        fullname: str = decoded_token.get("name")
        return {
            "username": username,
            "roles": roles,
            "email": email,
            "given_name": given_name,
            "family_name": family_name,
            "fullname": fullname,
        }

    def __decode_token(self, access_token: str) -> dict:
        """
        Decodes a JWT access token and validates it using a JWK and the
        valid audiences.
        :raises ExpiredSignatureError: If the access token is expired
        :raises HTTPException: If the access token was signed by an invalid provider,
        if the token audience is not valid,
        or if the claim dates are not valid
        For more details, please see:
        https://pyjwt.readthedocs.io/en/latest/api.html#exceptions
        :return CERN user data included inside the JWT access token
        :rtype dict
        """
        jwt_raw_token = self.jwt_regex.search(access_token)
        if jwt_raw_token:
            raw_token = jwt_raw_token[0]
            try:
                decoded_token: dict = jwt.decode(
                    jwt=raw_token,
                    key=self.jwk.key,
                    audience=self.valid_audiences,
                    algorithms=["RS256"],
                )
                return self.__token_to_user(decoded_token)
            except ExpiredSignatureError as expired_error:
                raise expired_error
            except InvalidTokenError as token_error:
                msg: str = (
                    "The provided JWT token is invalid - " f"Details: {token_error}"
                )
                error: dict = {"error": msg}
                response: Response = jsonify(error)
                response.status_code = 401
                raise HTTPException(description=msg, response=response)

        return None

    def __retrieve_token_from_session(self, session: SessionMixin) -> dict | None:
        """
        Retrieves the access and refresh tokens from a cookie via Flask session.
        Also, it attemps to refresh the access token if a refresh token is available
        in case the access token has expired.
        :return CERN user data included inside the JWT access token
        :return None if there is no access token available inside the session cookie
        or if there was an error while renewing the access token.
        This None value indicates that an interactive authentication is required
        :rtype dict | None
        """
        session_cookie: dict = session.get("token")
        if session_cookie:
            access_token: str = session_cookie.get("access_token")
            try:
                user_info: dict | None = self.__decode_token(access_token=access_token)
                return user_info
            except ExpiredSignatureError:
                # Try to refresh the token via refresh token claim
                try:
                    refresh_token: str = session_cookie.get("refresh_token")
                    new_token: dict = self.oauth_client.cern.fetch_access_token(
                        refresh_token=refresh_token, grant_type="refresh_token"
                    )
                    # Update the new token
                    new_access_token: str = new_token.get("access_token")
                    session["token"].update(new_token)
                    return self.__decode_token(access_token=new_access_token)
                except Exception:
                    # Maybe the refresh token expired
                    # Force an interactive authentication
                    session.pop("token")
                    return None
        return None

    def __retrieve_token_from_request(self, request: Request) -> dict | None:
        """
        Retrieves the access token from the Authorization header
        then it validates the token and retrieve the user data available inside .
        :return CERN user data included inside the JWT access token
        :return None if there is no access token available inside the Authorization header
        or if the access token is expired.
        This None value indicates that an interactive authentication is required
        :rtype dict | None
        """
        access_token = request.headers.get("Authorization")
        if access_token:
            try:
                return self.__decode_token(access_token=access_token)
            except ExpiredSignatureError:
                # We are not able to retrieve a refresh token
                # using the Authorization header
                return None
        return None

    def __call__(self, request: Request, session: SessionMixin) -> None:
        """
        Validate the access token and force a token request if necessary.
        :return None if there is a valid access token to authenticate the user or if the user is
        performing an authentication process.

        For more details, please see:
        https://flask.palletsprojects.com/en/2.2.x/api/?highlight=environ#flask.Flask.before_request

        Otherwise, it will redirect the user to sign in for an interactive authentication.
        """
        valid_auth_endpoints = ("oauth.auth", "oauth.callback")
        if request.endpoint in valid_auth_endpoints:
            # The user is performing an authentication process
            # This is usefull when you require to install the middleware
            # on the top of the Flask application. @before_request function
            # is called before any view, therefore, this could lead to infinite
            # redirect loops.
            return None

        user_data: dict = None
        user_data: dict | None = self.__retrieve_token_from_request(request=request)
        if user_data:
            session["user"] = user_data
            return None
        # Check if authentication comes from a cookie session
        user_data: dict | None = self.__retrieve_token_from_session(session=session)
        if user_data:
            session["user"] = user_data
            return None

        # Redirect to authentication endpoint:
        # Store this information inside the session
        original_destination: str = request.url
        session["next"] = original_destination

        redirect_uri: str = url_for(endpoint="oauth.auth")
        return redirect(location=redirect_uri)
