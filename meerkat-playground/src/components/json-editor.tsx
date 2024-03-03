import Editor from '@monaco-editor/react';

const EDITOR_LANGUAGE = 'json';

enum EDITOR_THEME {
  DARK = 'vs-dark',
  LIGHT = 'vs-light',
}

const EDITOR_OPTIONS = {
  formatOnPaste: true,
  formatOnType: true,
  minimap: { enabled: false },
  overviewRulerLanes: 0,
  padding: { top: 8 },
  renderLineHighlight: 'none' as const,
};

export const JsonEditor = ({
  json,
  onChange,
}: {
  json?: object;
  onChange: (input?: string) => void;
}) => {
  // Only valid valid JSON is allowed in the editor
  const editorVal = JSON.stringify(json, null, 2);

  return (
    <Editor
      className="border-y border-default bg-overlay"
      defaultLanguage={EDITOR_LANGUAGE}
      loading={null}
      onChange={onChange}
      options={EDITOR_OPTIONS}
      theme={EDITOR_THEME.DARK}
      value={editorVal}
    />
  );
};
