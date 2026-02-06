import os

FEATURE_FLAGS = {
    "ENABLE_JAVASCRIPT_CONTROLS": True,
}

# redundância proposital (em algumas versões esse toggle é lido também como config direta)
ENABLE_JAVASCRIPT_CONTROLS = True

MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")

PREFERRED_URL_SCHEME = "http"
SESSION_COOKIE_SECURE = False
WTF_CSRF_SSL_STRICT = False

TALISMAN_ENABLED = True

TALISMAN_CONFIG = {
    "force_https": False,
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src": ["'self'", "data:", "blob:", "https://*.mapbox.com"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'", "https://*.mapbox.com"],
        "style-src": ["'self'", "'unsafe-inline'", "https://*.mapbox.com"],
        "script-src": ["'self'", "'unsafe-eval'", "'unsafe-inline'"],
    },
}




