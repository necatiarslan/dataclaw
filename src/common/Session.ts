import * as ui from './UI';
import * as vscode from 'vscode';


export class Session implements vscode.Disposable {
    public static Current: Session | undefined = undefined;

    public Context: vscode.ExtensionContext;
    public ExtensionUri: vscode.Uri;
    public HostAppName: string = '';

    private _onDidChangeSession = new vscode.EventEmitter<void>();
    public readonly onDidChangeSession = this._onDidChangeSession.event;

    public constructor(context: vscode.ExtensionContext) {
        Session.Current = this;
        this.Context = context;
        this.ExtensionUri = context.extensionUri;
        this.HostAppName = vscode.env.appName;
    }

    public IsHostSupportLanguageTools(): boolean {
        const supportedHosts = ['Visual Studio Code', 'Visual Studio Code - Insiders', 'VSCodium'];
        return supportedHosts.includes(this.HostAppName);
    }

    public IsDebugMode(): boolean {
        return this.Context.extensionMode !== vscode.ExtensionMode.Production;
    }

    public dispose() {
        Session.Current = undefined;
        this._onDidChangeSession.dispose();
    }
}