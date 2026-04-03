#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wrapper to run format_auditor with proper encoding."""
import sys
import io

# Force UTF-8 stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Now import and run
sys.argv = ['format_auditor.py', '--quick', '--fix']
import runpy
runpy.run_path('format_auditor.py', run_name='__main__')
