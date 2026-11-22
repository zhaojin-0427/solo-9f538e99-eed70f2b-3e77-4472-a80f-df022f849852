"""WSGI 入口：gunicorn wsgi:app；直接 python wsgi.py 可起开发服务器。"""
from shard_planner import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
