from pathlib import Path
import os

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent

# Helpers for environment variables
def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "t", "yes", "y", "on")

def env_list(name: str, default: list[str] | None = None) -> list[str]:
    val = os.getenv(name)
    if not val:
        return default or []
    return [item.strip() for item in val.split(",") if item.strip()]

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default


# ------------------------------------------------------------------------------
# Core settings
# ------------------------------------------------------------------------------

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env_bool("DJANGO_DEBUG", True)

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-unsafe-secret-change-me")

if not DEBUG and SECRET_KEY == "dev-unsafe-secret-change-me":
    raise RuntimeError("DJANGO_SECRET_KEY must be set in production")

# Hosts and CSRF
ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS", default=(["*"] if DEBUG else []))
if not DEBUG and not ALLOWED_HOSTS:
    raise RuntimeError("DJANGO_ALLOWED_HOSTS must be set in production, e.g. 'example.com,.example.org'")

# CSRF trusted origins (must include scheme): e.g. "https://example.com,https://sub.example.com"
CSRF_TRUSTED_ORIGINS = env_list("DJANGO_CSRF_TRUSTED_ORIGINS", default=[])

# Application definition
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "scraper_app",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "scrapping.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],  # project-level templates directory
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "scrapping.wsgi.application"


# ------------------------------------------------------------------------------
# Database
# ------------------------------------------------------------------------------

# Default: SQLite for dev. For production, set DATABASE_URL or configure ENGINE/NAME/HOST/etc.
# Option A: Use DATABASE_URL with dj-database-url if available.
DATABASES = {
    "default": {
        "ENGINE": os.getenv("DB_ENGINE", "django.db.backends.sqlite3"),
        "NAME": os.getenv("DB_NAME", BASE_DIR / "db.sqlite3"),
        "USER": os.getenv("DB_USER", ""),
        "PASSWORD": os.getenv("DB_PASSWORD", ""),
        "HOST": os.getenv("DB_HOST", ""),
        "PORT": os.getenv("DB_PORT", ""),
    }
}

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    try:
        import dj_database_url  # type: ignore
        DATABASES["default"] = dj_database_url.parse(DATABASE_URL, conn_max_age=600)
    except Exception:
        # Fallback to explicit DB_* envs if dj-database-url is not installed
        pass


# ------------------------------------------------------------------------------
# Static and media files
# ------------------------------------------------------------------------------

STATIC_URL = "/static/"
STATICFILES_DIRS = [
    BASE_DIR / "static",  # looks in scrapping/static/
]
STATIC_ROOT = BASE_DIR / "staticfiles"  # for collectstatic on Render
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

# If you have additional project static assets (e.g., /assets), add:
# STATICFILES_DIRS = [BASE_DIR / "assets"]

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"  # ensure this is writable in production


# ------------------------------------------------------------------------------
# Password validation
# ------------------------------------------------------------------------------

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]


# ------------------------------------------------------------------------------
# Internationalization
# ------------------------------------------------------------------------------

LANGUAGE_CODE = os.getenv("DJANGO_LANGUAGE_CODE", "en-us")
TIME_ZONE = os.getenv("DJANGO_TIME_ZONE", "UTC")
USE_I18N = True
USE_TZ = True


# ------------------------------------------------------------------------------
# Security (enable when behind HTTPS)
# ------------------------------------------------------------------------------

# Set ENABLE_HTTPS=1 in production behind TLS
ENABLE_HTTPS = env_bool("ENABLE_HTTPS", default=not DEBUG)

SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", default=ENABLE_HTTPS)
SESSION_COOKIE_SECURE = env_bool("SESSION_COOKIE_SECURE", default=ENABLE_HTTPS)
CSRF_COOKIE_SECURE = env_bool("CSRF_COOKIE_SECURE", default=ENABLE_HTTPS)

SECURE_HSTS_SECONDS = int(os.getenv("SECURE_HSTS_SECONDS", "31536000" if ENABLE_HTTPS else "0"))
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", default=ENABLE_HTTPS)
SECURE_HSTS_PRELOAD = env_bool("SECURE_HSTS_PRELOAD", default=False)

SECURE_REFERRER_POLICY = os.getenv("SECURE_REFERRER_POLICY", "strict-origin-when-cross-origin")
X_FRAME_OPTIONS = os.getenv("X_FRAME_OPTIONS", "DENY")

# If behind a reverse proxy that sets X-Forwarded-Proto
USE_X_FORWARDED_HOST = env_bool("USE_X_FORWARDED_HOST", default=True)
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https") if ENABLE_HTTPS else None


# ------------------------------------------------------------------------------
# Cache (use Redis in production for CAPTCHA handoff between workers)
# ------------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": REDIS_URL,
            "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
            "TIMEOUT": env_int("CACHE_DEFAULT_TIMEOUT", 300),
        }
    }
else:
    # Local memory cache (OK for dev, not shared across processes)
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "unique-dev-cache",
            "TIMEOUT": env_int("CACHE_DEFAULT_TIMEOUT", 300),
        }
    }


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

LOG_LEVEL = os.getenv("DJANGO_LOG_LEVEL", "INFO")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "[{levelname}] {name}: {message}", "style": "{"},
        "verbose": {"format": "{asctime} [{levelname}] {name}: {message}", "style": "{", "datefmt": "%Y-%m-%d %H:%M:%S"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "verbose"},
    },
    "root": {"handlers": ["console"], "level": LOG_LEVEL},
    "loggers": {
        "django": {"handlers": ["console"], "level": LOG_LEVEL, "propagate": True},
        "scraper_app": {"handlers": ["console"], "level": LOG_LEVEL, "propagate": False},
    },
}


# ------------------------------------------------------------------------------
# Selenium / Scraper configuration (used by your scraper code)
# ------------------------------------------------------------------------------

SELENIUM_DEFAULT_WAIT = env_int("SELENIUM_DEFAULT_WAIT", 30)
CAPTCHA_WAIT_SECONDS = env_int("CAPTCHA_WAIT_SECONDS", 180)

# Path to ChromeDriver (optional if chromedriver is in PATH or you use webdriver-manager in dev)
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "")

# Optional: custom Chrome binary path (e.g., in containers)
CHROME_BINARY = os.getenv("CHROME_BIN") or os.getenv("CHROME_BINARY")

# Other scraper knobs
SCRAPER_MAX_LOGIN_ATTEMPTS = env_int("SCRAPER_MAX_LOGIN_ATTEMPTS", 10)
SCRAPER_MAX_CAPTCHA_ATTEMPTS = env_int("SCRAPER_MAX_CAPTCHA_ATTEMPTS", 10)


# ------------------------------------------------------------------------------
# Default primary key field type
# ------------------------------------------------------------------------------

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"