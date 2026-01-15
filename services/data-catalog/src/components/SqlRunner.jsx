import React, { useState } from 'react';
import axios from 'axios';
import { useAuth } from '../context/AuthContext';

const SqlRunner = ({ selectedProject, projectId }) => {
  const { token } = useAuth();
  const [sqlQuery, setSqlQuery] = useState("SELECT * FROM");
  const [sqlResults, setSqlResults] = useState(null);
  const [sqlError, setSqlError] = useState(null);
  const [isExecutingSql, setIsExecutingSql] = useState(false);

  const handleRunSQL = async () => {
    setSqlError(null);
    setSqlResults(null);
    setIsExecutingSql(true);

    try {
      console.log("--------------------------------------------------");
      console.log("[DEBUG] PREPARING TO RUN SQL");
      console.log("[DEBUG] Selected Project Name:", selectedProject);
      console.log("[DEBUG] Resolved Project ID:", projectId);

      if (!projectId) {
        console.error("[DEBUG] CRITICAL: Project ID is missing or undefined!");
      }
      console.log("--------------------------------------------------");

      const response = await axios.post(
        "http://localhost:8083/query/sql",
        {
          project_id: projectId,
          query: sqlQuery,
        },
        {
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
        },
      );

      setSqlResults(response.data);
    } catch (error) {
      console.error("SQL Execution failed", error);
      if (error.response && error.response.data) {
        console.error("[DEBUG] Backend Error Response:", error.response.data);
      }

      setSqlError(
        error.response?.data?.message ||
          error.response?.data?.error ||
          error.message ||
          "Unknown Error",
      );
    } finally {
      setIsExecutingSql(false);
    }
  };

  const getSqlHeaders = () => {
    if (!sqlResults || sqlResults.length === 0) return [];
    if (typeof sqlResults[0] !== "object") return ["Result"];
    return Object.keys(sqlResults[0]);
  };

  return (
    <div className="animate-fade-in">
      <h2 className="text-2xl font-bold mb-4 text-gray-800">
        Run SQL Query on{" "}
        <span className="text-blue-600">{selectedProject}</span>
      </h2>

      <div className="mb-4">
        <textarea
          className="w-full h-48 p-4 border rounded-lg font-mono text-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-gray-50"
          placeholder="SELECT * FROM table_name LIMIT 10..."
          value={sqlQuery}
          onChange={(e) => setSqlQuery(e.target.value)}
        />
      </div>

      <div className="flex justify-between items-center mb-6">
        <button
          onClick={handleRunSQL}
          disabled={isExecutingSql}
          className={`bg-blue-600 text-white font-bold py-2 px-6 rounded-lg shadow hover:bg-blue-700 transition ${
            isExecutingSql ? "opacity-50 cursor-not-allowed" : ""
          }`}
        >
          {isExecutingSql ? "Running..." : "Run Query"}
        </button>
        <div className="text-xs text-gray-400">
          Powered by DuckDB & Custom DFS
        </div>
      </div>

      {sqlError && (
        <div className="bg-red-50 border-l-4 border-red-500 p-4 mb-6">
          <div className="flex">
            <div className="flex-shrink-0">
              <svg
                className="h-5 w-5 text-red-500"
                viewBox="0 0 20 20"
                fill="currentColor"
              >
                <path
                  fillRule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                  clipRule="evenodd"
                />
              </svg>
            </div>
            <div className="ml-3">
              <p className="text-sm text-red-700">{sqlError}</p>
            </div>
          </div>
        </div>
      )}

      {sqlResults && (
        <div className="overflow-hidden border rounded-lg shadow-sm">
          <div className="bg-gray-100 px-6 py-3 border-b flex justify-between items-center">
            <span className="text-sm font-semibold text-gray-700">
              Query Results
            </span>
            <span className="text-xs text-gray-500">
              {sqlResults.length} rows
            </span>
          </div>
          {sqlResults.length > 0 ? (
            <div className="overflow-x-auto max-h-[500px]">
              <table className="w-full text-sm text-left text-gray-500">
                <thead className="text-xs text-gray-700 uppercase bg-gray-50 sticky top-0">
                  <tr>
                    {getSqlHeaders().map((header) => (
                      <th
                        key={header}
                        className="px-6 py-3 border-b"
                      >
                        {header}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sqlResults.map((row, rowIndex) => (
                    <tr
                      key={rowIndex}
                      className="bg-white border-b hover:bg-gray-50"
                    >
                      {getSqlHeaders().map((header) => (
                        <td
                          key={`${rowIndex}-${header}`}
                          className="px-6 py-4 whitespace-nowrap"
                        >
                          {typeof row[header] === "object"
                            ? JSON.stringify(row[header])
                            : row[header] !== null &&
                                row[header] !== undefined
                              ? row[header].toString()
                              : "NULL"}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center bg-white">
              <p className="text-gray-500">
                Query executed successfully but returned no rows.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default SqlRunner;
