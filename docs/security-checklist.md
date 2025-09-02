# Security Hardening Checklist for LijnDing Web UI

This checklist provides guidance for securely deploying the FastAPI backend component of the LijnDing Web UI. The default configuration is for local development only and **is not secure for production use**.

## Network and Server Configuration

-   [ ] **Bind to `localhost`**: By default, the FastAPI server binds to `127.0.0.1`. If you must expose the server to a network, never expose it directly to the internet. Always place it behind a reverse proxy like NGINX or Caddy.
-   [ ] **Use a Reverse Proxy**: A reverse proxy should handle SSL/TLS termination, rate limiting, and IP filtering.
-   [ ] **Enable HTTPS**: Configure your reverse proxy to enforce HTTPS. Use a tool like Let's Encrypt to obtain free SSL certificates.
-   [ ] **Run as a Non-Root User**: The FastAPI application should be run under a dedicated, unprivileged user account.

## FastAPI Application Security

-   [ ] **Implement Authentication**: The current backend has no authentication. Before deploying, implement a robust authentication mechanism.
    -   **Recommendation**: Use JWT (JSON Web Tokens) with short-lived access tokens and refresh tokens.
    -   Store hashed passwords only (e.g., using `passlib`).
-   [ ] **Implement Authorization / Access Control**: Ensure that endpoints are protected and that users can only access resources they are permitted to see. (e.g., if you add multi-user support).
-   [ ] **Enable CSRF Protection**: Implement Cross-Site Request Forgery protection, especially for any endpoints that modify state. Svelte and FastAPI can be configured to work with CSRF tokens.
-   [ ] **Input Validation**: FastAPI performs automatic data validation using Pydantic models for request bodies. Ensure all endpoints that accept data have strict Pydantic models. Do not trust any user-provided data.
-   [ ] **Set Secure Headers**: Use middleware to set security-related HTTP headers (e.g., `Content-Security-Policy`, `X-Content-Type-Options`, `X-Frame-Options`).
-   [ ] **Disable Debug Mode**: Ensure `debug=False` in any production Uvicorn or application settings.

## Logging and Monitoring

-   [ ] **Monitor Logs**: Regularly monitor the application and access logs for suspicious activity.
-   [ ] **Sanitize Log Data**: Ensure that sensitive information (e.g., passwords, API keys) is never written to the logs. The current persistence layer logs all item data, which may not be suitable for pipelines handling sensitive information. Consider adding a data sanitization step in the `PersistenceHooks` if required.

## Dependency Management

-   [ ] **Regularly Scan Dependencies**: Use a tool like `pip-audit` or GitHub's Dependabot to scan for vulnerabilities in your dependencies.
-   [ ] **Pin Dependencies**: Pin your production dependencies to specific versions to prevent unexpected and potentially insecure updates.

---

*Disclaimer: This checklist is a starting point and is not exhaustive. Security is an ongoing process.*
