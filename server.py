from flask import Flask, jsonify
import subprocess
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "service": "Air Quality Data Pipeline",
        "message": "Use /run-pipeline to trigger the pipeline"
    })

@app.route('/run-pipeline')
def run_pipeline():
    try:
        start_time = datetime.now()
        
        result = subprocess.run(
            ['python', 'pipeline.py'],
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return jsonify({
            "status": "success" if result.returncode == 0 else "error",
            "timestamp": start_time.isoformat(),
            "duration_seconds": duration,
            "return_code": result.returncode,
            "output": result.stdout[-500:] if result.stdout else "",
            "error": result.stderr[-500:] if result.stderr else ""
        })
    
    except subprocess.TimeoutExpired:
        return jsonify({
            "status": "timeout",
            "message": "Pipeline execution exceeded 60 minute timeout"
        }), 500
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
