import React from 'react';

const SchemaBrowser = ({ schemas, onViewData }) => {
  if (!schemas || schemas.length === 0) {
    return <p className="text-gray-500">No schemas found.</p>;
  }

  return (
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
                  onClick={() => onViewData(schema.name || schema)}
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
  );
};

export default SchemaBrowser;
