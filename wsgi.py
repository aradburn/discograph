from werkzeug.middleware.proxy_fix import ProxyFix

from discograph.app.prod_app import create_production_app

if __name__ == "__main__":
    app = create_production_app()
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    app.run(host="0.0.0.0", port=5000)
