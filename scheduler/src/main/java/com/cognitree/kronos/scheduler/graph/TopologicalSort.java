/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class TopologicalSort<V> {

    /**
     * The implementation here is basically an adjacency list, but instead
     * of an array of lists, a Map is used to map each vertex to its list of
     * adjacent vertices.
     */
    private Map<V, List<V>> neighbors = new HashMap<>();

    /**
     * String representation of graph.
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (V v : neighbors.keySet()) builder.append("\n    ").append(v).append(" -> ").append(neighbors.get(v));
        return builder.toString();
    }

    /**
     * Add a vertex to the graph. Nothing happens if vertex is already in graph.
     */
    public void add(V vertex) {
        if (neighbors.containsKey(vertex)) return;
        neighbors.put(vertex, new ArrayList<>());
    }

    /**
     * True iff graph contains vertex.
     */
    public boolean contains(V vertex) {
        return neighbors.containsKey(vertex);
    }

    /**
     * Add an edge to the graph; if either vertex does not exist, it's added.
     * This implementation allows the creation of multi-edges and self-loops.
     */
    public void add(V from, V to) {
        this.add(from);
        this.add(to);
        neighbors.get(from).add(to);
    }

    /**
     * Remove an edge from the graph. Nothing happens if no such edge.
     *
     * @throws IllegalArgumentException if either vertex doesn't exist.
     */
    public void remove(V from, V to) {
        if (!(this.contains(from) && this.contains(to)))
            throw new IllegalArgumentException("Nonexistent vertex");
        neighbors.get(from).remove(to);
    }

    /**
     * Report (as a Map) the out-degree of each vertex.
     */
    public Map<V, Integer> outDegree() {
        Map<V, Integer> result = new HashMap<>();
        for (V v : neighbors.keySet()) result.put(v, neighbors.get(v).size());
        return result;
    }

    /**
     * Report (as a Map) the in-degree of each vertex.
     */
    public Map<V, Integer> inDegree() {
        Map<V, Integer> result = new HashMap<>();
        for (V v : neighbors.keySet()) result.put(v, 0);       // All in-degrees are 0
        for (V from : neighbors.keySet()) {
            for (V to : neighbors.get(from)) {
                result.put(to, result.get(to) + 1);           // Increment in-degree
            }
        }
        return result;
    }

    /**
     * Report (as a List) the topological sort of the vertices; null for no such sort.
     */
    public List<V> sort() {
        Map<V, Integer> degree = inDegree();
        // Determine all vertices with zero in-degree
        Stack<V> zeroVerts = new Stack<>();        // Stack as good as any here
        for (V v : degree.keySet()) {
            if (degree.get(v) == 0) zeroVerts.push(v);
        }
        // Determine the topological order
        List<V> result = new ArrayList<>();
        while (!zeroVerts.isEmpty()) {
            V v = zeroVerts.pop();                  // Choose a vertex with zero in-degree
            result.add(v);                          // Vertex v is next in topol order
            // "Remove" vertex v by updating its neighbors
            for (V neighbor : neighbors.get(v)) {
                degree.put(neighbor, degree.get(neighbor) - 1);
                // Remember any vertices that now have zero in-degree
                if (degree.get(neighbor) == 0) zeroVerts.push(neighbor);
            }
        }
        // Check that we have used the entire graph (if not, there was a cycle)
        if (result.size() != neighbors.size()) return null;
        return result;
    }

    /**
     * True iff graph is a dag (directed acyclic graph).
     */
    public boolean isDag() {
        return sort() != null;
    }
}