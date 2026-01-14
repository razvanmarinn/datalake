import { useState, useEffect } from "react";
import axios from "axios";

function App() {
  const [token, setToken] = useState(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const [projects, setProjects] = useState({});
  const [selectedProject, setSelectedProject] = useState(null);

  const [schemas, setSchemas] = useState([]);
  const [activeSchema, setActiveSchema] = useState(null);
  const [schemaData, setSchemaData] = useState([]);

  const [query, setQuery] = useState("");
  const [queryResult, setQueryResult] = useState(null);

  const [datasets, setDatasets] = useState([]);
  const [search, setSearch] = useState("");

  // SQL Runner State
  const [activeTab, setActiveTab] = useState("browse"); // 'browse' | 'sql'
  const [sqlQuery, setSqlQuery] = useState("SELECT * FROM");
  const [sqlResults, setSqlResults] = useState(null);
  const [sqlError, setSqlError] = useState(null);
  const [isExecutingSql, setIsExecutingSql] = useState(false);

  useEffect(() => {
    if (token) {
      axios
        .get("/mock-data.json")
        .then((res) => setDatasets(res.data))
        .catch(() => {});
    }
  }, [token]);

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post("http://localhost:8082/login/", {
        username,
        password,
      });

      setToken(response.data.token);

      const projResp = await axios.get(
        `http://localhost:8081/projects/by-username/${username}`,
        {
          headers: { Authorization: `Bearer ${response.data.token}` },
        },
      );

      // --- DEBUG START ---
      console.log(
        "[DEBUG] Login Successful. Projects Response:",
        projResp.data,
      );
      console.log(
        "[DEBUG] Projects Map being set to state:",
        projResp.data.projects,
      );
      // --- DEBUG END ---

      setProjects(projResp.data.projects);
    } catch (error) {
      console.error("Login failed", error);
      alert("Login failed");
    }
  };

  const handleProjectClick = async (projectName) => {
    // --- DEBUG START ---
    console.log("[DEBUG] User clicked project:", projectName);
    console.log(
      "[DEBUG] ID associated with this project:",
      projects[projectName],
    );
    // --- DEBUG END ---

    setSelectedProject(projectName);
    setSchemas([]);
    setActiveSchema(null);
    setSchemaData([]);
    setActiveTab("browse"); // Reset to browse on project switch

    try {
      const schemaResp = await axios.get(
        `http://localhost:8081/${projectName}/schemas`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setSchemas(schemaResp.data);
    } catch (error) {
      console.error("Failed to fetch schemas", error);
      alert("Failed to fetch schemas for " + projectName);
    }
  };

  const handleViewData = async (schemaName) => {
    setActiveSchema(schemaName);
    setSchemaData([]);

    try {
      const response = await axios.get(
        `http://localhost:8086/projects/${selectedProject}/schemas/${schemaName}/data`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setSchemaData(response.data);
    } catch (error) {
      console.error("Failed to fetch schema data", error);
      alert("Failed to fetch data for " + schemaName);
    }
  };

  const handleQuery = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.get(
        `http://localhost:8086/query?file_name=${query}`,
        {
          headers: { Authorization: `Bearer ${token}` },
        },
      );
      setQueryResult(JSON.stringify(response.data, null, 2));
    } catch (error) {
      console.error("Query failed", error);
      setQueryResult("Query failed");
    }
  };

  const handleRunSQL = async () => {
    setSqlError(null);
    setSqlResults(null);
    setIsExecutingSql(true);

    try {
      const projectId = projects[selectedProject];

      // --- DEBUG START ---
      console.log("--------------------------------------------------");
      console.log("[DEBUG] PREPARING TO RUN SQL");
      console.log("[DEBUG] Selected Project Name:", selectedProject);
      console.log("[DEBUG] Resolved Project ID:", projectId);
      console.log("[DEBUG] Full Projects Map:", projects);

      if (!projectId) {
        console.error("[DEBUG] CRITICAL: Project ID is missing or undefined!");
      }
      console.log("--------------------------------------------------");
      // --- DEBUG END ---

      // Note: Using port 8083 per your previous errors.
      // Change to 8080 if you want to use the Gateway.
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

      // Assuming response.data is the array of results or response.data.data
      setSqlResults(response.data);
    } catch (error) {
      console.error("SQL Execution failed", error);

      // Log the specific response data from backend if available
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

  const filtered = datasets.filter(
    (d) => d.name && d.name.toLowerCase().includes(search.toLowerCase()),
  );

  if (!token) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white p-8 rounded-2xl shadow-md w-full max-w-sm">
          <h1 className="text-2xl font-bold mb-6 text-center">Login</h1>
          <form onSubmit={handleLogin}>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Username
              </label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="shadow border rounded w-full py-2 px-3"
              />
            </div>
            <div className="mb-6">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="shadow border rounded w-full py-2 px-3"
              />
            </div>
            <button
              type="submit"
              className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded w-full"
            >
              Sign In
            </button>
          </form>
        </div>
      </div>
    );
  }

  const getDataHeaders = () => {
    if (!schemaData || schemaData.length === 0) return [];
    return Object.keys(schemaData[0]);
  };

  const getSqlHeaders = () => {
    if (!sqlResults || sqlResults.length === 0) return [];
    if (typeof sqlResults[0] !== "object") return ["Result"];
    return Object.keys(sqlResults[0]);
  };

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto bg-white p-6 rounded-2xl shadow">
        {/* Header */}
        <div className="flex justify-between items-center mb-8 border-b pb-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">Data Catalog</h1>
            <p className="text-gray-500">Welcome, {username}</p>
          </div>
          <button
            onClick={() => {
              setToken(null);
              setProjects({});
              setSchemas([]);
              setSelectedProject(null);
            }}
            className="bg-red-500 hover:bg-red-600 text-white font-semibold py-2 px-6 rounded transition"
          >
            Logout
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          {/* Sidebar: Projects */}
          <div className="md:col-span-1 bg-gray-50 p-4 rounded-xl border h-fit">
            <h2 className="text-xl font-bold mb-4 text-gray-700">
              Your Projects
            </h2>
            <div className="flex flex-col gap-2">
              {Object.entries(projects).map(([projectName, projectID]) => (
                <button
                  key={projectName}
                  onClick={() => handleProjectClick(projectName)}
                  className={`text-left px-4 py-3 rounded-lg font-medium transition ${
                    selectedProject === projectName
                      ? "bg-blue-600 text-white shadow-md"
                      : "bg-white hover:bg-blue-50 text-gray-700 border"
                  }`}
                >
                  {projectName}
                </button>
              ))}
            </div>
          </div>

          {/* Main Content Area */}
          <div className="md:col-span-3">
            {selectedProject ? (
              <>
                {/* Tabs */}
                <div className="flex space-x-4 mb-6 border-b">
                  <button
                    onClick={() => setActiveTab("browse")}
                    className={`pb-2 px-4 font-semibold transition ${
                      activeTab === "browse"
                        ? "text-blue-600 border-b-2 border-blue-600"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    Browse Data
                  </button>
                  <button
                    onClick={() => setActiveTab("sql")}
                    className={`pb-2 px-4 font-semibold transition ${
                      activeTab === "sql"
                        ? "text-blue-600 border-b-2 border-blue-600"
                        : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    SQL Runner
                  </button>
                </div>

                {/* Tab: Browse Data */}
                {activeTab === "browse" && (
                  <>
                    {!activeSchema && (
                      <div className="mb-8 animate-fade-in">
                        <h2 className="text-2xl font-bold mb-4 text-gray-800">
                          Schemas for{" "}
                          <span className="text-blue-600">
                            {selectedProject}
                          </span>
                        </h2>

                        {schemas.length > 0 ? (
                          <div className="overflow-x-auto border rounded-lg shadow-sm">
                            <table className="w-full text-sm text-left text-gray-500">
                              <thead className="text-xs text-gray-700 uppercase bg-gray-100">
                                <tr>
                                  <th className="px-6 py-3">Schema Name</th>
                                  <th className="px-6 py-3">Created At</th>
                                  <th className="px-6 py-3">Actions</th>
                                </tr>
                              </thead>
                              <tbody>
                                {schemas.map((schema, index) => (
                                  <tr
                                    key={index}
                                    className="bg-white border-b hover:bg-gray-50"
                                  >
                                    <td className="px-6 py-4 font-medium text-gray-900">
                                      {schema.name || schema}
                                    </td>
                                    <td className="px-6 py-4">
                                      {schema.created_at || "N/A"}
                                    </td>
                                    <td className="px-6 py-4">
                                      <button
                                        onClick={() =>
                                          handleViewData(schema.name || schema)
                                        }
                                        className="bg-blue-100 text-blue-700 py-1 px-3 rounded hover:bg-blue-200 font-medium transition"
                                      >
                                        View Data
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <p className="text-gray-500">No schemas found.</p>
                        )}
                      </div>
                    )}

                    {activeSchema && (
                      <div className="mb-8">
                        <div className="flex items-center justify-between mb-4">
                          <h2 className="text-2xl font-bold text-gray-800">
                            Data:{" "}
                            <span className="text-green-600">
                              {activeSchema}
                            </span>
                          </h2>
                          <button
                            onClick={() => setActiveSchema(null)}
                            className="text-gray-500 hover:text-gray-700 underline"
                          >
                            ← Back to Schemas
                          </button>
                        </div>

                        {schemaData.length > 0 ? (
                          <div className="overflow-x-auto border rounded-lg shadow-sm max-h-[500px] overflow-y-auto">
                            <table className="w-full text-sm text-left text-gray-500">
                              <thead className="text-xs text-gray-700 uppercase bg-gray-100 sticky top-0">
                                <tr>
                                  {getDataHeaders().map((header) => (
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
                                {schemaData.map((row, rowIndex) => (
                                  <tr
                                    key={rowIndex}
                                    className="bg-white border-b hover:bg-gray-50"
                                  >
                                    {getDataHeaders().map((header) => (
                                      <td
                                        key={`${rowIndex}-${header}`}
                                        className="px-6 py-4"
                                      >
                                        {typeof row[header] === "object"
                                          ? JSON.stringify(row[header])
                                          : row[header]}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <div className="p-8 text-center bg-gray-50 rounded-lg border border-dashed border-gray-300">
                            <p className="text-gray-500">
                              No data found for this schema (or loading...)
                            </p>
                          </div>
                        )}
                      </div>
                    )}
                  </>
                )}

                {/* Tab: SQL Runner */}
                {activeTab === "sql" && (
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
                )}
              </>
            ) : (
              // Empty State (No Project Selected)
              <div className="h-full flex items-center justify-center bg-gray-50 border-2 border-dashed border-gray-200 rounded-xl p-12">
                <div className="text-center">
                  <h3 className="mt-2 text-sm font-semibold text-gray-900">
                    No project selected
                  </h3>
                  <p className="mt-1 text-sm text-gray-500">
                    Select a project from the sidebar to view schemas or run
                    queries.
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
