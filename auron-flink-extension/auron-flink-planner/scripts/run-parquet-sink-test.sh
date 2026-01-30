#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Run ParquetSink verification test on standalone Flink cluster
# This validates end-to-end native execution with zero conversions
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(cd "$MODULE_DIR/../.." && pwd)"

echo ""
echo "=========================================="
echo "Auron ParquetSink Verification Test"
echo "=========================================="
echo ""

# Set Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Check if dependencies are built
if [ ! -d "$MODULE_DIR/target/lib" ]; then
    echo "📦 Building dependencies (first time only)..."
    cd "$MODULE_DIR"
    "$PROJECT_ROOT/build/apache-maven-3.9.12/bin/mvn" dependency:copy-dependencies -DskipTests \
        -Pflink-1.18 -Pscala-2.12 > /dev/null 2>&1
    echo "✅ Dependencies built"
fi

# Check if test classes are compiled
if [ ! -f "$MODULE_DIR/target/test-classes/org/apache/auron/flink/planner/AuronParquetSinkVerificationTest.class" ]; then
    echo "📦 Compiling test classes..."
    cd "$MODULE_DIR"
    "$PROJECT_ROOT/build/apache-maven-3.9.12/bin/mvn" test-compile -DskipTests \
        -Pflink-1.18 -Pscala-2.12 > /dev/null 2>&1
    echo "✅ Test classes compiled"
fi

cd "$MODULE_DIR"

echo ""
echo "🚀 Running ParquetSink Verification Test..."
echo ""
echo "⚠️  WATCH FOR THIS LOG MESSAGE:"
echo "   'Detected end-to-end native execution with ParquetSink'"
echo ""
echo "If you see it: Data stayed in Arrow format (zero conversions)!"
echo ""
echo "=========================================="
echo ""

# Run the test
java -cp "target/classes:target/test-classes:target/lib/*" \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     org.apache.auron.flink.planner.AuronParquetSinkVerificationTest

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ✅ ✅  TEST PASSED  ✅ ✅ ✅"
else
    echo "❌ TEST FAILED"
fi
echo ""

exit $EXIT_CODE
