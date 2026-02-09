import os

FEATURE_FLAGS = {
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,

    # algumas instalações antigas ainda dependem desse toggle para UI de filtros
    "DASHBOARD_FILTERS_EXPERIMENTAL": True,
}

ENABLE_JAVASCRIPT_CONTROLS = True

MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")

PREFERRED_URL_SCHEME = "http"
SESSION_COOKIE_SECURE = False
WTF_CSRF_SSL_STRICT = False

# se você não precisa forçar talisman/csp agora, recomendo desligar pra não bloquear UI sem querer
TALISMAN_ENABLED = False
