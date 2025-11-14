import { useEffect, useState } from 'react';
import axios from 'axios';

interface EventContent {
  event_type: string;
  data: Record<string, any>;
}

interface Event {
  name: string;
  content: EventContent;
}

interface ApiResponse {
  message?: string;
  error?: string;
  details?: string;
}

const RabbitMQFrontend = () => {
  const [events, setEvents] = useState<Event[]>([]);
  const [selectedEvents, setSelectedEvents] = useState<string[]>([]);
  const [selectedEventContent, setSelectedEventContent] = useState<Event | null>(null);
  const [response, setResponse] = useState<ApiResponse | null>(null);

  // Get publisher URL from environment variable
  const PUBLISHER_URL = import.meta.env.VITE_PUBLISHER_URL || 'http://localhost:8000/publish';

  // Load all JSON files dynamically from /events
  useEffect(() => {
    const importEvents = async () => {
      try {
        const eventModules = import.meta.glob<{ default: EventContent }>('../events/*.json');
        const loadedEvents = await Promise.all(
          Object.keys(eventModules).map(async (path) => {
            const data = await eventModules[path]();
            return { name: path.split('/').pop() || '', content: data.default };
          })
        );
        setEvents(loadedEvents);
      } catch (error) {
        console.error('Error loading events:', error);
      }
    };
    importEvents();
  }, []);

  const handleSelectEvent = (eventName: string) => {
    if (selectedEvents.includes(eventName)) {
      setSelectedEvents(selectedEvents.filter((e) => e !== eventName));
    } else {
      setSelectedEvents([...selectedEvents, eventName]);
    }
  };

  const handleViewEvent = (event: Event) => {
    setSelectedEventContent(event);
  };

  const handleSendEvent = async (eventList: string[]) => {
    try {
      setResponse(null);
      const payload = eventList.map((eName) => {
        const e = events.find((ev) => ev.name === eName);
        return e?.content;
      });

      const res = await axios.post(PUBLISHER_URL, payload, {
        headers: {
          'Content-Type': 'application/json',
        },
      });
      setResponse(res.data);
    } catch (error) {
      console.error('Error sending event:', error);
      if (axios.isAxiosError(error)) {
        setResponse({ 
          error: 'Failed to send events.', 
          details: error.response?.data || error.message 
        });
      } else {
        setResponse({ 
          error: 'Failed to send events.', 
          details: 'Unknown error occurred' 
        });
      }
    }
  };

  return (
    <div className="min-h-screen min-w-screen bg-gray-100 text-gray-800 p-6">
      <h1 className="text-3xl font-bold mb-4 text-center">RabbitMQ Event Publisher</h1>
      <div className="grid grid-cols-3 gap-4">
        {/* Event List */}
        <div className="bg-white rounded-xl shadow-md p-4">
          <h2 className="text-xl font-semibold mb-2">Available Events</h2>
          <ul className="space-y-2">
            {events.map((event, idx) => (
              <li
                key={idx}
                className={`p-2 rounded cursor-pointer border ${
                  selectedEvents.includes(event.name)
                    ? 'bg-blue-100 border-blue-400'
                    : 'hover:bg-gray-100 border-gray-200'
                }`}
                onClick={() => handleSelectEvent(event.name)}
              >
                {event.name}
                <button
                  className="ml-3 text-sm text-blue-600 underline"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleViewEvent(event);
                  }}
                >
                  View
                </button>
              </li>
            ))}
          </ul>
        </div>

        {/* Event Viewer */}
        <div className="bg-white rounded-xl shadow-md p-4 overflow-auto">
          <h2 className="text-xl font-semibold mb-2">Event Preview</h2>
          {selectedEventContent ? (
            <pre className="text-sm bg-gray-50 p-3 rounded overflow-x-auto">
              {JSON.stringify(selectedEventContent.content, null, 2)}
            </pre>
          ) : (
            <p className="text-gray-500">Select an event to preview its contents.</p>
          )}

          {selectedEvents.length > 0 && (
            <button
              onClick={() => handleSendEvent(selectedEvents)}
              className="mt-4 bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
            >
              Send Selected ({selectedEvents.length})
            </button>
          )}
        </div>

        {/* Response Display */}
        <div className="bg-white rounded-xl shadow-md p-4 overflow-auto">
          <h2 className="text-xl font-semibold mb-2">Response</h2>
          {response ? (
            <pre className="text-sm bg-gray-50 p-3 rounded overflow-x-auto">
              {JSON.stringify(response, null, 2)}
            </pre>
          ) : (
            <p className="text-gray-500">No response yet.</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default RabbitMQFrontend;
