import { useState, useEffect } from "react";
import axios from "axios";

function App() {
  const [token, setToken] = useState(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const [projects, setProjects] = useState({});
  const [query, setQuery] = useState("");
  const [queryResult, setQueryResult] = useState(null);

  // For your mock dataset search
  const [datasets, setDatasets] = useState([]);
  const [search, setSearch] = useState("");

  useEffect(() => {
    if (token) {
      axios.get("/mock-data.json").then((res) => setDatasets(res.data));
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

      // Fetch projects for this username
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

  const handleGetFileList = async (projectID) => {
    try {
      const response = await axios.get(
        `http://localhost:8086/get_file_list/${projectID}`,
        {
          headers: { Authorization: `Bearer ${token}` },
        }
      );

      setQueryResult(JSON.stringify(response.data, null, 2));
    } catch (error) {
      console.error("Failed to fetch file list", error);
      setQueryResult("Failed to fetch file list");
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
    d.name.toLowerCase().includes(search.toLowerCase())
  );

  if (!token) {
    // ---- LOGIN SCREEN ----
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="bg-white p-8 rounded-2xl shadow-md w-full max-w-sm">
          <h1 className="text-2xl font-bold mb-6 text-center">Login</h1>
          <form onSubmit={handleLogin}>
            {/* Username */}
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

            {/* Password */}
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
              className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
            >
              Sign In
            </button>
          </form>
        </div>
      </div>
    );
  }

  // ---- MAIN APP SCREEN ----
  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-5xl mx-auto bg-white p-6 rounded-2xl shadow">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Data Catalog</h1>

          <button
            onClick={() => {
              setToken(null);
              setProjects({});
            }}
            className="bg-red-500 hover:bg-red-700 text-white font-bold py-2 px-4 rounded"
          >
            Logout
          </button>
        </div>

        {/* ---- PROJECT LIST ---- */}
        <div className="mb-6">
          <h2 className="text-lg font-bold mb-2">Your Projects</h2>

          <div className="flex flex-wrap gap-2">
            {Object.entries(projects).map(([projectName, projectID]) => (
              <button
                key={projectID}
                onClick={() => handleGetFileList(projectID)}
                className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
              >
                {projectName}
              </button>
            ))}
          </div>

          {Object.keys(projects).length === 0 && (
            <p className="text-gray-500">No projects found.</p>
          )}
        </div>

        {/* ---- QUERY SECTION ---- */}
        <div className="mb-6">
          <form onSubmit={handleQuery} className="flex gap-2">
            <input
              type="text"
              placeholder="Enter file name to query..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="border rounded-lg p-2 w-full"
            />

            <button
              type="submit"
              className="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded"
            >
              Query
            </button>
          </form>

          {queryResult && (
            <div className="mt-4 p-4 bg-gray-100 rounded">
              <h3 className="font-bold">Result:</h3>
              <pre>{queryResult}</pre>
            </div>
          )}
        </div>

        {/* ---- DATASET SEARCH AND TABLE ---- */}
        <input
          type="text"
          placeholder="Search datasets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="border rounded-lg p-2 w-full mb-4"
        />

        <table className="table-auto w-full border-collapse border border-gray-200 text-sm">
          <thead className="bg-gray-100">
            <tr>
              <th className="border p-2 text-left">Name</th>
              <th className="border p-2 text-left">Description</th>
              <th className="border p-2 text-left">Format</th>
              <th className="border p-2 text-left">Size</th>
            </tr>
          </thead>

          <tbody>
            {filtered.map((d) => (
              <tr key={d.id} className="hover:bg-gray-50">
                <td className="border p-2 font-medium">{d.name}</td>
                <td className="border p-2">{d.description}</td>
                <td className="border p-2">{d.format}</td>
                <td className="border p-2">{d.size}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {filtered.length === 0 && (
          <p className="text-gray-500 mt-4 text-center">No datasets found.</p>
        )}
      </div>
    </div>
  );
}

export default App;
