import { useState, useEffect } from "react";
import axios from "axios";

function App() {
  const [datasets, setDatasets] = useState([]);
  const [search, setSearch] = useState("");

  useEffect(() => {
    axios.get("/mock-data.json").then((res) => setDatasets(res.data));
  }, []);

  const filtered = datasets.filter((d) =>
    d.name.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-5xl mx-auto bg-white p-6 rounded-2xl shadow">
        <h1 className="text-2xl font-bold mb-6">📊 Data Catalog</h1>

        <input
          type="text"
          placeholder="Search datasets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="border rounded-lg p-2 w-full mb-4 focus:outline-none focus:ring-2 focus:ring-blue-400"
        />

        <table className="table-auto w-full border-collapse border border-gray-200 text-sm">
          <thead className="bg-gray-100">
            <tr>
              <th className="border border-gray-200 p-2 text-left">Name</th>
              <th className="border border-gray-200 p-2 text-left">Description</th>
              <th className="border border-gray-200 p-2 text-left">Format</th>
              <th className="border border-gray-200 p-2 text-left">Size</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((d) => (
              <tr key={d.id} className="hover:bg-gray-50">
                <td className="border border-gray-200 p-2 font-medium">{d.name}</td>
                <td className="border border-gray-200 p-2">{d.description}</td>
                <td className="border border-gray-200 p-2">{d.format}</td>
                <td className="border border-gray-200 p-2">{d.size}</td>
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
