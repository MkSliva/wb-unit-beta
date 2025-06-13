import React, { useState, useEffect } from "react";

const CardChanges = ({ goBack }) => {
  const [options, setOptions] = useState([]);
  const [newOpt, setNewOpt] = useState("");

  const load = async () => {
    try {
      const resp = await fetch("http://localhost:8000/api/card_change_options");
      if (resp.ok) {
        const list = await resp.json();
        setOptions(list);
      }
    } catch (e) {
      console.error("Failed to load options", e);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const addOption = async () => {
    if (!newOpt) return;
    await fetch("http://localhost:8000/api/card_change_options", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: newOpt }),
    });
    setNewOpt("");
    load();
  };

  const delOption = async (name) => {
    await fetch(`http://localhost:8000/api/card_change_options?name=${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    load();
  };

  return (
    <div className="p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-xl font-bold">Изменения карточек</h1>
        <button onClick={goBack} className="px-4 py-2 bg-gray-300 rounded">Назад</button>
      </div>
      <div className="mb-4">
        <input
          type="text"
          value={newOpt}
          onChange={(e) => setNewOpt(e.target.value)}
          className="border p-2 mr-2"
        />
        <button onClick={addOption} className="px-2 py-1 bg-blue-500 text-white rounded">
          Добавить
        </button>
      </div>
      <ul className="list-disc pl-5">
        {options.map((o) => (
          <li key={o} className="mb-1">
            {o}{" "}
            <button className="text-red-600 ml-2" onClick={() => delOption(o)}>
              удалить
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default CardChanges;
