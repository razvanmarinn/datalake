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

  useEffect(() => {
    if (token) {
      axios.get("/mock-data.json").then((res) => setDatasets(res.data)).catch(() => {});
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
        }
      );

      setProjects(projResp.data.projects);
    } catch (error) {
      console.error("Login failed", error);
      alert("Login failed");
    }
  };

  const handleProjectClick = async (projectName) => {
    setSelectedProject(projectName);
    setSchemas([]); 
    setActiveSchema(null); 
    setSchemaData([]);    

    try {
      const schemaResp = await axios.get(
        `http://localhost:8081/${projectName}/schemas`, 
        {
          headers: { Authorization: `Bearer ${token}` },
        }
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
        }
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
        }
      );
      setQueryResult(JSON.stringify(response.data, null, 2));
    } catch (error) {
      console.error("Query failed", error);
      setQueryResult("Query failed");
    }
  };

  const filtered = datasets.filter((d) =>
    d.name && d.name.toLowerCase().includes(search.toLowerCase())
  );

  if (!token) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white p-8 rounded-2xl shadow-md w-full max-w-sm">
          <h1 className="text-2xl font-bold mb-6 text-center">Login</h1>
          <form onSubmit={handleLogin}>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">Username</label>
              <input type="text" value={username} onChange={(e) => setUsername(e.target.value)} className="shadow border rounded w-full py-2 px-3" />
            </div>
            <div className="mb-6">
              <label className="block text-gray-700 text-sm font-bold mb-2">Password</label>
              <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} className="shadow border rounded w-full py-2 px-3" />
            </div>
            <button type="submit" className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded w-full">Sign In</button>
          </form>
        </div>
      </div>
    );
  }


  const getDataHeaders = () => {
    if (!schemaData || schemaData.length === 0) return [];
    return Object.keys(schemaData[0]);
  };

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto bg-white p-6 rounded-2xl shadow">
        
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

          <div className="md:col-span-1 bg-gray-50 p-4 rounded-xl border h-fit">
            <h2 className="text-xl font-bold mb-4 text-gray-700">Your Projects</h2>
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

          <div className="md:col-span-3">
            
            {selectedProject && !activeSchema && (
              <div className="mb-8 animate-fade-in">
                <h2 className="text-2xl font-bold mb-4 text-gray-800">
                  Schemas for <span className="text-blue-600">{selectedProject}</span>
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
                          <tr key={index} className="bg-white border-b hover:bg-gray-50">
                            <td className="px-6 py-4 font-medium text-gray-900">{schema.name || schema}</td>
                            <td className="px-6 py-4">{schema.created_at || "N/A"}</td>
                            <td className="px-6 py-4">
                              <button 
                                onClick={() => handleViewData(schema.name || schema)}
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
                    Data: <span className="text-green-600">{activeSchema}</span>
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
                            <th key={header} className="px-6 py-3 border-b">{header}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {schemaData.map((row, rowIndex) => (
                          <tr key={rowIndex} className="bg-white border-b hover:bg-gray-50">
                            {getDataHeaders().map((header) => (
                              <td key={`${rowIndex}-${header}`} className="px-6 py-4">
                                {typeof row[header] === 'object' 
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
                    <p className="text-gray-500">No data found for this schema (or loading...)</p>
                  </div>
                )}
              </div>
            )}

            <hr className="my-8"/>

            <div className="opacity-75 hover:opacity-100 transition">
              <h3 className="text-lg font-bold mb-2 text-gray-500 uppercase text-xs tracking-wider">Debug Tools</h3>
              <div className="bg-gray-50 p-4 rounded-lg border">
                <h4 className="font-semibold mb-2">Ad-hoc File Query</h4>
                <form onSubmit={handleQuery} className="flex gap-2">
                  <input
                    type="text"
                    placeholder="Enter raw file name..."
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    className="flex-1 border rounded px-3 py-1 text-sm"
                  />
                  <button type="submit" className="bg-gray-600 text-white px-4 py-1 rounded text-sm hover:bg-gray-700">Check</button>
                </form>
                {queryResult && <pre className="mt-2 text-xs bg-gray-800 text-green-400 p-2 rounded overflow-auto">{queryResult}</pre>}
              </div>
            </div>

          </div>
        </div>
      </div>
    </div>
  );
}

export default App;