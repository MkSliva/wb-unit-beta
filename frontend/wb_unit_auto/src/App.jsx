import React, { useState } from "react";
import Dashboard from "./Dashboard";
import UnitEconomics from "./UnitEconomics";

function App() {
  const [page, setPage] = useState("dashboard");
  return (
    <div>
      {page === "dashboard" ? (
        <Dashboard openEconomics={() => setPage("economics")} />
      ) : (
        <UnitEconomics goBack={() => setPage("dashboard")} />
      )}
    </div>
  );
}

export default App;
