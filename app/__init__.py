"""
Application factory and blueprint registration.
"""
from .core import app  # reuse the original app instance

def create_app():
    from .blueprints.misc import bp as misc_bp
    from .blueprints.import_wizard import bp as import_wizard_bp
    from .blueprints.requirements import bp as requirements_bp

    # Register once only
    if 'misc' not in app.blueprints:
        app.register_blueprint(misc_bp)
    if 'import_wizard' not in app.blueprints:
        app.register_blueprint(import_wizard_bp)
    if 'requirements' not in app.blueprints:
        app.register_blueprint(requirements_bp)

    return app

# Expose for WSGI servers
application = create_app()
