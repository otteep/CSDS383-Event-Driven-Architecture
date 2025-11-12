import React, { useEffect, useMemo, useState } from "react";

export const APIForm: React.FC = () => {
  type Resource =
    | "products"
    | "suppliers"
    | "categories"
    | "images"
    | "links";

  type Method =
    | "create"
    | "read"
    | "list"
    | "update"
    | "delete"
    | "link"
    | "unlink";


  const [resource, setResource] = useState<Resource>("products");
  const [method, setMethod] = useState<Method>("create");
  const [dark, setDark] = useState<boolean>(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [fields, setFields] = useState<Record<string, string>>({
    // product
    name: "",
    description: "",
    quantity: "",
    price: "",
    product_id: "",
    // supplier
    supplier_id: "",
    contact: "",
    // category
    category_id: "",
    // image
    image_id: "",
    image_url: "",
  });

  useEffect(() => {
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    setDark(prefersDark);
  }, []);

  useEffect(() => {
    setResult(null);
    setError(null);
  }, [resource, method]);

  const baseUrl = useMemo(() => {
    return import.meta.env.VITE_API_BASE_URL || "http://localhost:8080/api";
  }, []);

  const resetFieldsFor = () => {
    setFields((prev) => ({
      ...prev,
      name: "",
      description: "",
      quantity: "",
      price: "",
      product_id: "",
      supplier_id: "",
      contact: "",
      category_id: "",
      image_id: "",
      image_url: "",
    }));
  };

  useEffect(() => resetFieldsFor(), [resource, method]);

  function updateField(k: string, v: string) {
    setFields((p) => ({ ...p, [k]: v }));
  }

  const buildRequest = () => {
    let url = "";
    let opts: RequestInit = { headers: {} } as RequestInit;

    const pid = encodeURIComponent(fields.product_id || "");
    const sid = encodeURIComponent(fields.supplier_id || "");
    const cid = encodeURIComponent(fields.category_id || "");
    const imgid = encodeURIComponent(fields.image_id || "");

    if (resource === "products") {
      switch (method) {
        case "create":
          url = `${baseUrl}/products`;
          opts.method = "POST";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({
            name: fields.name,
            description: fields.description || undefined,
            quantity: fields.quantity ? Number(fields.quantity) : undefined,
            price: fields.price ? Number(fields.price) : undefined,
          });
          break;
        case "read":
          url = `${baseUrl}/products/${pid}`;
          opts.method = "GET";
          break;
        case "list":
          url = `${baseUrl}/products`;
          opts.method = "GET";
          break;
        case "update":
          url = `${baseUrl}/products/${pid}`;
          opts.method = "PUT";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({
            name: fields.name || undefined,
            description: fields.description || undefined,
            quantity: fields.quantity ? Number(fields.quantity) : undefined,
            price: fields.price ? Number(fields.price) : undefined,
          });
          break;
        case "delete":
          url = `${baseUrl}/products/${pid}`;
          opts.method = "DELETE";
          break;
        default:
          throw new Error("Unsupported method for products");
      }
    }

    if (resource === "suppliers") {
      switch (method) {
        case "create":
          url = `${baseUrl}/suppliers`;
          opts.method = "POST";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ name: fields.name, contact: fields.contact });
          break;
        case "read":
          url = `${baseUrl}/suppliers/${sid}`;
          opts.method = "GET";
          break;
        case "list":
          url = `${baseUrl}/suppliers`;
          opts.method = "GET";
          break;
        case "update":
          url = `${baseUrl}/suppliers/${sid}`;
          opts.method = "PUT";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ name: fields.name || undefined, contact: fields.contact || undefined });
          break;
        case "delete":
          url = `${baseUrl}/suppliers/${sid}`;
          opts.method = "DELETE";
          break;
        default:
          throw new Error("Unsupported method for suppliers");
      }
    }

    if (resource === "categories") {
      switch (method) {
        case "create":
          url = `${baseUrl}/categories`;
          opts.method = "POST";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ name: fields.name, description: fields.description });
          break;
        case "read":
          url = `${baseUrl}/categories/${cid}`;
          opts.method = "GET";
          break;
        case "list":
          url = `${baseUrl}/categories`;
          opts.method = "GET";
          break;
        case "update":
          url = `${baseUrl}/categories/${cid}`;
          opts.method = "PUT";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ name: fields.name || undefined, description: fields.description || undefined });
          break;
        case "delete":
          url = `${baseUrl}/categories/${cid}`;
          opts.method = "DELETE";
          break;
        default:
          throw new Error("Unsupported method for categories");
      }
    }

    if (resource === "images") {
      switch (method) {
        case "create":
          url = `${baseUrl}/images`;
          opts.method = "POST";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ 
            product_id: fields.product_id, 
            url: fields.image_url  
          });
          break;
        case "read":
          url = `${baseUrl}/images/${imgid}`;
          opts.method = "GET";
          break;
        case "list":
          url = `${baseUrl}/images`;
          opts.method = "GET";
          break;
        case "update":
          url = `${baseUrl}/images/${imgid}`;
          opts.method = "PUT";
          opts.headers = { "Content-Type": "application/json" };
          opts.body = JSON.stringify({ 
            product_id: fields.product_id || undefined, 
            url: fields.image_url || undefined  
          });
          break;
        case "delete":
          url = `${baseUrl}/images/${imgid}`;
          opts.method = "DELETE";
          break;
        default:
          throw new Error("Unsupported method for images");
      }
    }

    if (resource === "links") {
      // Links handles linking/unlinking suppliers and categories to products
      if (method === "link" || method === "unlink") {
        // Determine if linking supplier or category by which id field is filled
        if (fields.supplier_id) {
          url = `${baseUrl}/products/${pid}/suppliers/${sid}`;
        } else if (fields.category_id) {
          url = `${baseUrl}/products/${pid}/categories/${cid}`;
        } else {
          throw new Error("Provide supplier_id or category_id to link/unlink.");
        }
        opts.method = method === "link" ? "POST" : "DELETE";
      } else {
        throw new Error("links resource only supports link/unlink");
      }
    }

    return { url, opts };
  };

  const handleSubmit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    setError(null);
    setResult(null);
    setLoading(true);

    try {

      
      if (["read", "update", "delete"].includes(method)) {
        if (resource === "products" && !fields.product_id) throw new Error("product_id is required");
        if (resource === "suppliers" && !fields.supplier_id) throw new Error("supplier_id is required");
        if (resource === "categories" && !fields.category_id) throw new Error("category_id is required");
        if (resource === "images" && !fields.image_id) throw new Error("image_id is required");
      }

      if (resource === "links") {
        if (!fields.product_id) throw new Error("product_id is required to link/unlink");
        if (!fields.supplier_id && !fields.category_id) throw new Error("supplier_id or category_id is required to link/unlink");
      }

      const { url, opts } = buildRequest();

      const resp = await fetch(url, opts);
      const text = await resp.text();

      try {
        const json = JSON.parse(text);
        setResult(JSON.stringify(json, null, 2));
      } catch (_) {
        setResult(text);
      }

      if (!resp.ok) {
        setError(`Request failed: ${resp.status} ${resp.statusText}`);
      }
    } catch (err: any) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
    }
  };

  const methodsForResource = (r: Resource): Method[] => {
    if (r === "links") return ["link", "unlink"];
    return ["create", "read", "list", "update", "delete"];
    
  };
  
    useEffect(() => {
    const allowed = methodsForResource(resource);
      if (!allowed.includes(method)) {
        setMethod(allowed[0]);
      }
    }, [resource, method]);

  const renderFields = () => {
    switch (resource) {
      case "products":
        return (
          <>
            {(method === "create" || method === "update") && (
              <>
                <label className="block text-sm font-medium">Name</label>
                <input value={fields.name} onChange={(e) => updateField("name", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1" placeholder="Product name" />

                <label className="block text-sm font-medium">Description</label>
                <input value={fields.description} onChange={(e) => updateField("description", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="Short description" />

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium">Quantity</label>
                    <input value={fields.quantity} onChange={(e) => updateField("quantity", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="0" type="number" />
                  </div>
                  <div>
                    <label className="block text-sm font-medium">Price</label>
                    <input value={fields.price} onChange={(e) => updateField("price", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="0.00" type="number" step="0.01" />
                  </div>
                </div>
              </>
            )}

            {(method === "read" || method === "update" || method === "delete") && (
              <div className="">
                <label className="block text-sm font-medium">Product ID (UUID)</label>
                <input value={fields.product_id} onChange={(e) => updateField("product_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="uuid" />
              </div>
            )}
          </>
        );

      case "suppliers":
        return (
          <>
            {(method === "create" || method === "update") && (
              <>
                <label className="block text-sm font-medium">Name</label>
                <input value={fields.name} onChange={(e) => updateField("name", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="Supplier name" />

                <label className="block text-sm font-medium">Contact</label>
                <input value={fields.contact} onChange={(e) => updateField("contact", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="email@company.com" type="email" />
              </>
            )}

            {(method === "read" || method === "update" || method === "delete") && (
              <>
                <label className="block text-sm font-medium">Supplier ID (UUID)</label>
                <input value={fields.supplier_id} onChange={(e) => updateField("supplier_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="uuid" />
              </>
            )}
          </>
        );

      case "categories":
        return (
          <>
            {(method === "create" || method === "update") && (
              <>
                <label className="block text-sm font-medium">Name</label>
                <input value={fields.name} onChange={(e) => updateField("name", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="Category name" />

                <label className="block text-sm font-medium">Description</label>
                <input value={fields.description} onChange={(e) => updateField("description", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="Optional description" />
              </>
            )}

            {(method === "read" || method === "update" || method === "delete") && (
              <>
                <label className="block text-sm font-medium">Category ID (UUID)</label>
                <input value={fields.category_id} onChange={(e) => updateField("category_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="uuid" />
              </>
            )}
          </>
        );

      case "images":
        return (
          <>
            {(method === "create" || method === "update") && (
              <>
                <label className="block text-sm font-medium">Product ID (UUID)</label>
                <input value={fields.product_id} onChange={(e) => updateField("product_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="uuid" />

                <label className="block text-sm font-medium">Image URL</label>
                <input value={fields.image_url} onChange={(e) => updateField("image_url", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="https://..." />
              </>
            )}

            {(method === "read" || method === "update" || method === "delete") && (
              <>
                <label className="block text-sm font-medium">Image ID (UUID)</label>
                <input value={fields.image_id} onChange={(e) => updateField("image_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="uuid" />
              </>
            )}
          </>
        );

      case "links":
        return (
          <>
            <label className="block text-sm font-medium">Product ID (UUID)</label>
            <input value={fields.product_id} onChange={(e) => updateField("product_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="product uuid" />

            <label className="block text-sm font-medium pt-2">Supplier ID (UUID)</label>
            <input value={fields.supplier_id} onChange={(e) => updateField("supplier_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="supplier uuid" />

            <label className="block text-sm font-medium pt-2">Category ID (UUID)</label>
            <input value={fields.category_id} onChange={(e) => updateField("category_id", e.target.value)} className="w-full border-4 border-indigo-200 rounded-lg px-2 py-1"  placeholder="category uuid" />
          </>
        );

      default:
        return null;
    }
  };

  return (
    <div className={`${dark ? "dark" : ""}`}>
      <div className="min-h-screen min-w-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 transition-colors duration-200">
        <div className="max-w-6xl mx-auto p-6 h-full flex flex-col">
          <header className="flex items-center justify-between mb-6">
            <h1 className="text-2xl font-semibold">Microservices Architecture API</h1>
            <div className="flex items-center gap-3">

              <span className="text-sm text-muted">Base: {baseUrl}</span>
            </div>
          </header>

          <main className="flex-1 grid grid-cols-1 lg:grid-cols-3 gap-6 mt-20">
            <section className="col-span-1 lg:col-span-1 p-4 rounded-2xl shadow-md bg-gray-50 dark:bg-gray-800">
              <label className="block text-sm font-medium">Resource</label>
              <select value={resource} onChange={(e) => {setResource(e.target.value as Resource);setMethod("create");}} className="input" >
                <option className="bg-slate-500" value="products">Products</option>
                <option className="bg-slate-500" value="suppliers">Suppliers</option>
                <option className="bg-slate-500" value="categories">Categories</option>
                <option className="bg-slate-500" value="images">Images</option>
                <option className="bg-slate-500" value="links">Link / Unlink</option>
              </select>

              <label className="block text-sm font-medium mt-4">Method</label>
              <select value={method} onChange={(e) => setMethod(e.target.value as Method)} className="input" >
                {methodsForResource(resource).map((m) => (
                  <option className="bg-slate-500" value={m} key={m}>
                    {m}
                  </option>
                ))}
              </select>



              <div className="mt-6 flex gap-2">
                <button onClick={() => { setFields((f) => ({ ...f, name: "Demo product", description: "Demo description", quantity: "10", price: "9.99", contact: "demo@gmail.com" })); }} className="btn-secondary">
                  Fill demo product
                </button>
                <button onClick={() => { setFields({ name: "", description: "", quantity: "", price: "", product_id: "", supplier_id: "", contact: "", category_id: "", image_id: "", image_url: "" }); }} className="btn-ghost">
                  Clear
                </button>
              </div>
            </section>

            <section className="col-span-1 lg:col-span-2 p-6 rounded-2xl shadow-lg bg-white dark:bg-gray-900">
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">{renderFields()}</div>

                <div className="flex items-center gap-3">
                  <button type="submit" disabled={loading} className="btn-primary">
                    {loading ? "Sending..." : "Send Request"}
                  </button>

                  <button type="button" onClick={() => { setResult(null); setError(null); }} className="btn-ghost">
                    Clear Result
                  </button>
                </div>
              </form>

              <div className="mt-6">
                <h3 className="text-lg font-medium">Response</h3>
                <div className="mt-2 p-3 rounded-lg bg-gray-100 dark:bg-gray-800 min-h-[120px] overflow-auto">
                  {error && <pre className="text-red-400">{error}</pre>}
                  {result && <pre className="whitespace-pre-wrap">{result}</pre>}
                  {!result && !error && <p className="text-sm text-muted">No response yet.</p>}
                </div>
              </div>
            </section>
          </main>
        </div>
      </div>

      <style>{`
        .input { width: 100%; padding: 0.5rem; border-radius: 0.5rem; border: 1px solid rgba(0,0,0,0.08); background: transparent }
        .input:focus { outline: none; box-shadow: 0 0 0 4px rgba(99,102,241,0.08) }
        .btn-primary { padding: 0.5rem 1rem; border-radius: 0.75rem; background: #6366f1; color: white }
        .btn-primary:disabled { opacity: 0.6 }
        .btn-secondary { padding: 0.4rem 0.9rem; border-radius: 0.6rem; border: 1px solid rgba(0,0,0,0.08) }
        .btn-ghost { padding: 0.4rem 0.9rem; border-radius: 0.6rem; background: transparent }
        .text-muted { color: rgba(107,114,128,1) }
      `}</style>
    </div>
  );
};

export default APIForm;
